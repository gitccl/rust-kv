use crate::{Result, ThreadPool};
use log::{info, warn};
use std::{
    panic::{self, AssertUnwindSafe},
    sync::{
        mpsc::{self, Receiver},
        Arc, Mutex,
    },
    thread,
};

type Job = Box<dyn FnOnce() + Send + 'static>;
enum Message {
    NewJob(Job),
    Terminate,
}

pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads_num: usize) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = mpsc::channel();
        let mut workers = Vec::with_capacity(threads_num);
        let receiver = Arc::new(Mutex::new(receiver));

        for i in 0..threads_num {
            workers.push(Worker::new(i + 1, receiver.clone()));
        }
        Ok(SharedQueueThreadPool { workers, sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Message::NewJob(Box::new(job))).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.sender.send(Message::Terminate).unwrap();
        }

        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                handle.join().unwrap();
            }
        }

        info!("thread pool exited");
    }
}

pub struct Worker {
    handle: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: usize, receiver: Arc<Mutex<Receiver<Message>>>) -> Worker {
        let handle = thread::spawn(move || loop {
            let msg = receiver.lock().unwrap().recv().unwrap();
            match msg {
                Message::NewJob(job) => {
                    if let Err(err) = panic::catch_unwind(AssertUnwindSafe(job)) {
                        warn!("[thread {}] job panic: {:?}", id, err);
                    }
                }
                Message::Terminate => {
                    break;
                }
            };
        });
        Worker {
            handle: Some(handle),
        }
    }
}
