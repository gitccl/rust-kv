use std::{
    collections::HashMap,
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::{KvError, Result};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

/// The `KvStore` stores string key/value pairs.
pub struct KvStore {
    dir_path: PathBuf,
    index: HashMap<String, RecordInfo>,
    readers: HashMap<u64, BufReader<File>>,
    current_writer: BufWriterWithPosition<File>,
    current_file_id: u64,
    uncompacted: u64,
}

impl KvStore {
    /// Opens a `KvStore` with the given dir_path.
    ///
    /// This will create a new directory if the given one does not exist.
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = dir_path.into();
        fs::create_dir_all(&dir_path)?;

        let mut index = HashMap::new();
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

        Ok(KvStore {
            dir_path,
            index,
            readers,
            current_writer,
            current_file_id,
            uncompacted,
        })
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(record) = self.index.get(&key) {
            let buf_reader = self.readers.get_mut(&record.file_id).unwrap();
            buf_reader.seek(SeekFrom::Start(record.offset))?;
            let reader = buf_reader.take(record.length);
            // the command in the log must be a Set cmd, otherwise the log is corrupted
            if let Command::Set(_, value) = serde_json::from_reader(reader)? {
                Ok(Some(value))
            } else {
                Err(KvError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
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

    /// Removes a given key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let old_record = self.index.remove(&key).expect("key not found");
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
        let compact_file_id = self.current_file_id + 1;
        let mut compact_writer = self.new_log_writer(compact_file_id)?;
        let mut prev_offset = 0;
        for record in self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&record.file_id)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(record.offset))?;
            let mut record_reader = reader.take(record.length);
            io::copy(&mut record_reader, &mut compact_writer)?;
            let curr_offset = compact_writer.get_offset();
            *record = RecordInfo {
                file_id: compact_file_id,
                offset: prev_offset,
                length: curr_offset - prev_offset,
            };
            prev_offset = curr_offset;
        }
        compact_writer.flush()?;

        for (&file_id, _) in &self.readers {
            fs::remove_file(log_path(&self.dir_path, file_id))?;
        }
        self.readers.clear();

        self.current_file_id += 2;
        self.readers
            .insert(compact_file_id, self.new_log_reader(compact_file_id)?);
        self.current_writer = self.new_log_writer(self.current_file_id)?;
        self.readers.insert(
            self.current_file_id,
            self.new_log_reader(self.current_file_id)?,
        );
        self.uncompacted = 0;
        Ok(())
    }

    fn new_log_writer(&self, file_id: u64) -> Result<BufWriterWithPosition<File>> {
        let path = log_path(&self.dir_path, file_id);
        Ok(BufWriterWithPosition::new(
            OpenOptions::new().create(true).append(true).open(&path)?,
        )?)
    }

    fn new_log_reader(&self, file_id: u64) -> Result<BufReader<File>> {
        let path = log_path(&self.dir_path, file_id);
        Ok(BufReader::new(File::open(path)?))
    }

    /// Recover the KvStore from the dir_path
    ///
    /// Return the maximum file_id that has been used
    fn recover(
        dir_path: &Path,
        index: &mut HashMap<String, RecordInfo>,
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
                        uncompacted += index.remove(&key).map(|record| record.length).unwrap_or(0);
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

fn log_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{}.log", file_id))
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
struct RecordInfo {
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
