use std::{
    collections::{hash_map::Entry, HashMap},
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use dashmap::DashMap;
use log::warn;
use serde::{Deserialize, Serialize};

use crate::{KvEngine, KvError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
#[derive(Clone)]
pub struct KvStore {
    index: Arc<DashMap<String, RecordInfo>>,
    reader: KvReader,
    writer: Arc<Mutex<KvWriter>>,
}

impl KvStore {
    /// Opens a `KvStore` with the given dir_path.
    ///
    /// This will create a new directory if the given one does not exist.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = dir_path.into();
        fs::create_dir_all(&dir_path)?;

        let mut index = DashMap::new();
        let mut readers = HashMap::new();
        let (current_file_id, uncompacted) = Self::recover(&dir_path, &mut index, &mut readers)?;

        let log_path = log_path(&dir_path, current_file_id);
        let current_writer = BufWriterWithPosition::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_path)?,
        )?;

        if !readers.contains_key(&current_file_id) {
            readers.insert(current_file_id, BufReader::new(File::open(&log_path)?));
        }

        let dir_path = Arc::new(dir_path);
        let index = Arc::new(index);
        let safe_point = Arc::new(AtomicU64::new(0));

        let reader = KvReader {
            dir_path: dir_path.clone(),
            readers,
            safe_point,
        };

        let writer = KvWriter {
            dir_path: dir_path.clone(),
            index: index.clone(),
            reader: reader.clone(),
            current_writer,
            current_file_id,
            uncompacted,
        };

        Ok(KvStore {
            index,
            reader,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    /// Recover the KvStore from the dir_path
    ///
    /// Return the maximum file_id that has been used
    fn recover(
        dir_path: &Path,
        index: &mut DashMap<String, RecordInfo>,
        readers: &mut HashMap<u64, BufReader<File>>,
    ) -> Result<(u64, u64)> {
        let mut file_ids: Vec<u64> = fs::read_dir(dir_path)?
            .flat_map(|dir| -> Result<_> { Ok(dir?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|file_name| file_name.trim_end_matches(".log"))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();

        file_ids.sort_unstable();

        let mut uncompacted = 0;
        for &file_id in &file_ids {
            let mut prev_offset = 0;
            let path = log_path(dir_path, file_id);
            let mut reader = BufReader::new(File::open(&path)?);
            let mut iters =
                serde_json::Deserializer::from_reader(&mut reader).into_iter::<Command>();
            // cannot use for loop, it will move the ownership of iters
            while let Some(cmd) = iters.next() {
                let curr_offset = iters.byte_offset() as u64;
                match cmd? {
                    Command::Set(key, _) => {
                        uncompacted += index
                            .insert(
                                key,
                                RecordInfo {
                                    file_id,
                                    offset: prev_offset,
                                    length: curr_offset - prev_offset,
                                },
                            )
                            .map(|record| record.length)
                            .unwrap_or(0);
                    }
                    Command::Remove(key) => {
                        uncompacted += index
                            .remove(&key)
                            .map(|(_, record)| record.length)
                            .unwrap_or(0);
                        uncompacted += curr_offset - prev_offset;
                    }
                }
                prev_offset = curr_offset;
            }
            readers.insert(file_id, reader);
        }

        Ok((*file_ids.last().unwrap_or(&0), uncompacted))
    }
}

impl KvEngine for KvStore {
    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(record) = self.index.get(&key) {
            self.reader.read_value(record.value())
        } else {
            Ok(None)
        }
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.writer.lock().unwrap().set(key, value)
    }

    /// Removes a given key.
    fn remove(&mut self, key: String) -> Result<()> {
        self.writer.lock().unwrap().remove(key)
    }
}

pub struct KvReader {
    dir_path: Arc<PathBuf>,
    readers: HashMap<u64, BufReader<File>>,
    // generation of the latest compaction file
    safe_point: Arc<AtomicU64>,
}

impl KvReader {
    fn remove_stale_reader(&mut self) {
        let readers = &mut self.readers;
        let compact_file_id = self.safe_point.load(Ordering::SeqCst);
        while !readers.is_empty() {
            let file_id = *readers.keys().next().unwrap();
            if file_id >= compact_file_id {
                break;
            }
            readers.remove(&file_id);
        }
    }

    /// Read the log file at the given `CommandPos`.
    pub fn read_and<F, R>(&mut self, record: &RecordInfo, func: F) -> Result<R>
    where
        F: FnOnce(io::Take<&mut BufReader<File>>) -> Result<R>,
    {
        self.remove_stale_reader();

        let readers = &mut self.readers;
        if let Entry::Vacant(entry) = readers.entry(record.file_id) {
            entry.insert(new_log_reader(&self.dir_path, record.file_id)?);
        }

        let buf_reader = readers.get_mut(&record.file_id).unwrap();
        buf_reader.seek(SeekFrom::Start(record.offset))?;
        func(buf_reader.take(record.length))
    }

    pub fn read_value(&mut self, record: &RecordInfo) -> Result<Option<String>> {
        self.read_and(record, |reader| {
            // the command in the log must be a Set cmd, otherwise the log is corrupted
            if let Command::Set(_, value) = serde_json::from_reader(reader)? {
                Ok(Some(value))
            } else {
                Err(KvError::UnexpectedCommandType)
            }
        })
    }

    pub fn remove_stale_file(&mut self, compact_file_id: u64) {
        let readers = &mut self.readers;
        let file_ids: Vec<u64> = readers
            .iter()
            .map(|(&file_id, _)| file_id)
            .filter(|&file_id| file_id < compact_file_id)
            .collect();

        for file_id in file_ids {
            readers.remove(&file_id);
            if let Err(err) = fs::remove_file(log_path(&self.dir_path, file_id)) {
                warn!("remove file error: {}", err);
            }
        }
    }
}

impl Clone for KvReader {
    fn clone(&self) -> Self {
        Self {
            dir_path: self.dir_path.clone(),
            readers: HashMap::new(),
            safe_point: self.safe_point.clone(),
        }
    }
}

pub struct KvWriter {
    dir_path: Arc<PathBuf>,
    index: Arc<DashMap<String, RecordInfo>>,
    reader: KvReader,
    current_writer: BufWriterWithPosition<File>,
    current_file_id: u64,
    uncompacted: u64,
}

impl KvWriter {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set(key, value);
        let offset = self.current_writer.get_offset();
        serde_json::to_writer(&mut self.current_writer, &cmd)?;
        self.current_writer.flush()?;
        let record = RecordInfo {
            file_id: self.current_file_id,
            offset,
            length: self.current_writer.get_offset() - offset,
        };
        if let Command::Set(key, _) = cmd {
            self.uncompacted += self
                .index
                .insert(key, record)
                .map(|record| record.length)
                .unwrap_or(0);
        }

        if self.uncompacted >= COMPACTION_THRESHOLD {
            self.compact()?;
        }
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let (_, old_record) = self.index.remove(&key).expect("key not found");
            let cmd = Command::Remove(key);
            let offset = self.current_writer.get_offset();
            serde_json::to_writer(&mut self.current_writer, &cmd)?;
            self.current_writer.flush()?;
            self.uncompacted += self.current_writer.get_offset() - offset;
            self.uncompacted += old_record.length;

            if self.uncompacted >= COMPACTION_THRESHOLD {
                self.compact()?;
            }
            Ok(())
        } else {
            Err(KvError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log.
    fn compact(&mut self) -> Result<()> {
        // compact writer use current_file_id + 1
        let mut prev_offset = 0;
        let compact_file_id = self.current_file_id + 1;
        let mut compact_writer = new_log_writer(&self.dir_path, compact_file_id)?;
        let mut new_records = HashMap::with_capacity(self.index.len());

        for entry in self.index.iter_mut() {
            self.reader.read_and(entry.value(), |mut reader| {
                io::copy(&mut reader, &mut compact_writer)?;
                Ok(())
            })?;
            let curr_offset = compact_writer.get_offset();
            new_records.insert(
                entry.key().clone(),
                RecordInfo {
                    file_id: compact_file_id,
                    offset: prev_offset,
                    length: curr_offset - prev_offset,
                },
            );
            prev_offset = curr_offset;
        }
        compact_writer.flush()?;
        for (key, rec) in new_records {
            self.index.insert(key, rec);
        }

        self.reader
            .safe_point
            .store(compact_file_id, Ordering::SeqCst);
        self.reader.remove_stale_file(compact_file_id);

        self.current_file_id += 2;
        self.current_writer = new_log_writer(&self.dir_path, self.current_file_id)?;
        self.uncompacted = 0;
        Ok(())
    }
}

fn log_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{}.log", file_id))
}

fn new_log_writer(dir_path: &Path, file_id: u64) -> Result<BufWriterWithPosition<File>> {
    let path = log_path(dir_path, file_id);
    Ok(BufWriterWithPosition::new(
        OpenOptions::new().create(true).append(true).open(&path)?,
    )?)
}

fn new_log_reader(dir_path: &Path, file_id: u64) -> Result<BufReader<File>> {
    let path = log_path(dir_path, file_id);
    Ok(BufReader::new(File::open(path)?))
}

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    // set key value
    Set(String, String),
    // remove key
    Remove(String),
}

/// Represents the position and length of a json-serialized record in the log.
#[derive(Clone)]
pub struct RecordInfo {
    file_id: u64,
    offset: u64,
    length: u64,
}

/// A BufWriter with write position.
struct BufWriterWithPosition<T: Write + Seek> {
    offset: u64,
    writer: BufWriter<T>,
}

impl<T: Write + Seek> BufWriterWithPosition<T> {
    fn new(mut inner: T) -> Result<Self> {
        let offset = inner.seek(SeekFrom::End(0))?;
        Ok(BufWriterWithPosition {
            offset,
            writer: BufWriter::new(inner),
        })
    }

    fn get_offset(&self) -> u64 {
        self.offset
    }
}

impl<T: Write + Seek> Write for BufWriterWithPosition<T> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let write_size = self.writer.write(buf)?;
        self.offset += write_size as u64;
        Ok(write_size)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
