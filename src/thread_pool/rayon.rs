use std::sync::Arc;

use crate::ThreadPool;

#[derive(Clone)]
pub struct RayonThreadPool {
    pool: Arc<rayon::ThreadPool>,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads_num: usize) -> crate::Result<Self>
    where
        Self: Sized,
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads_num)
            .build()?;
        Ok(RayonThreadPool {
            pool: Arc::new(pool),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}
