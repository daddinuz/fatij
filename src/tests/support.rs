use crate::*;
use std::sync::mpsc;

pub(super) const WAIT: Duration = Duration::from_secs(5);
// Only negative assertions use this window; positive progress uses channel handshakes.
const QUIET: Duration = Duration::from_millis(50);

// Report each implementation separately so a pool failure cannot hide a task failure.
macro_rules! pool_tests {
    ($name:ident, |$pool:pat_param| $body:block) => {
        mod $name {
            use super::*;

            fn run($pool: Pool) $body

            #[test]
            fn thread_pool() {
                run(Pool::Thread(ThreadPool::new()));
            }

            #[test]
            fn task() {
                run(Pool::Task(Task::new(run_job)));
            }
        }
    };
}

pub(super) use pool_tests;

pub(super) type TestTask = Task<Box<dyn Job>, (), fn(Box<dyn Job>)>;

pub(super) fn run_job(job: Box<dyn Job>) {
    job();
}

pub(super) fn successful<O>((_, result): TaskOutput<O>) -> O {
    result.expect("task input failed")
}

pub(super) enum Pool {
    Thread(ThreadPool),
    Task(TestTask),
}

impl Pool {
    pub(super) fn both() -> [Self; 2] {
        [
            Self::Thread(ThreadPool::new()),
            Self::Task(Task::new(run_job)),
        ]
    }

    pub(super) fn scale(&mut self, count: usize) {
        match self {
            Self::Thread(pool) => pool.scale(count),
            Self::Task(task) => task.scale(count),
        }
    }

    pub(super) fn execute(&self, job: impl Job) {
        match self {
            Self::Thread(pool) => pool.execute(job),
            Self::Task(task) => {
                task.send(Box::new(job));
            }
        }
    }

    pub(super) fn join(self) {
        match self {
            Self::Thread(pool) => pool.join(),
            Self::Task(task) => task.join(),
        }
    }
}

pub(super) struct Gate {
    pub(super) started: mpsc::Receiver<()>,
    pub(super) release: mpsc::Sender<()>,
}

impl Gate {
    pub(super) fn wait_started(&self) {
        self.started.recv_timeout(WAIT).expect("job did not start");
    }

    pub(super) fn release(self) {
        self.release.send(()).expect("job stopped before release");
    }
}

pub(super) fn blocked_job() -> (impl Job, Gate) {
    let (started_tx, started) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let job = move || {
        started_tx.send(()).unwrap();
        release_rx.recv_timeout(WAIT).expect("job was not released");
    };
    (job, Gate { started, release })
}

pub(super) fn in_background<T: Send + 'static>(
    operation: impl FnOnce() -> T + Send + 'static,
) -> (mpsc::Receiver<T>, JoinHandle<()>) {
    let (started_tx, started_rx) = mpsc::channel();
    let (finished_tx, finished_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        let result = operation();
        let _ = finished_tx.send(result);
    });
    started_rx.recv_timeout(WAIT).unwrap();
    (finished_rx, handle)
}

pub(super) fn assert_blocked<T>(finished: &mpsc::Receiver<T>) {
    match finished.recv_timeout(QUIET) {
        Err(mpsc::RecvTimeoutError::Timeout) => {}
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("operation disconnected or panicked while it should still be blocked");
        }
        Ok(_) => panic!("operation completed while it should still be blocked"),
    }
}

// Bound scenarios that exercise blocking APIs, and preserve assertion failures from the child.
pub(super) fn bounded<T: Send + 'static>(operation: impl FnOnce() -> T + Send + 'static) -> T {
    let (finished, handle) =
        in_background(move || panic::catch_unwind(AssertUnwindSafe(operation)));
    let result = finished
        .recv_timeout(WAIT)
        .expect("test scenario did not finish before its timeout");
    handle.join().unwrap();
    match result {
        Ok(value) => value,
        Err(payload) => panic::resume_unwind(payload),
    }
}

pub(super) struct DropCounter(pub(super) Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

pub(super) struct PanickingPayload {
    pub(super) drops: Arc<AtomicUsize>,
    pub(super) replacement_drops: Option<Arc<AtomicUsize>>,
}

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
        if let Some(drops) = &self.replacement_drops {
            panic::panic_any(Self {
                drops: drops.clone(),
                replacement_drops: None,
            });
        }
        panic!("panic payload destructor");
    }
}
