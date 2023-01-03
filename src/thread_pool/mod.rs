use crate::Result;

mod naive;
mod rayon;
mod shared_queue;

/// The trait that all thread pools should implement.
pub trait ThreadPool: Clone + Send + 'static {
    /// Creates a new thread pool, immediately spawning the specified number of threads.
    fn new(threads_num: usize) -> Result<Self>
    where
        Self: Sized;

    /// Spawns a function into the thread pool.
    /// Spawning always succeeds, thread pool should ignore function panics.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub use self::rayon::RayonThreadPool;
pub use naive::NaiveThreadPool;
pub use shared_queue::SharedQueueThreadPool;
