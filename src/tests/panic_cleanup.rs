use super::support::*;
use crate::*;
use std::sync::mpsc;

fn assert_worker_recovers_from_payload(mut pool: Pool, payload: impl Send + UnwindSafe + 'static) {
    pool.scale(1);
    let (failed_tx, failed_rx) = mpsc::channel();
    pool.execute(move || {
        failed_tx.send(thread::current().id()).unwrap();
        panic::panic_any(payload);
    });
    let (completed_tx, completed_rx) = mpsc::channel();
    pool.execute(move || completed_tx.send(thread::current().id()).unwrap());
    pool.join();

    let worker = failed_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(completed_rx.recv_timeout(WAIT).unwrap(), worker);
}

#[test]
fn thread_pool_payload_destructor_does_not_kill_worker() {
    let drops = Arc::new(AtomicUsize::new(0));
    assert_worker_recovers_from_payload(
        Pool::Thread(ThreadPool::new()),
        PanickingPayload {
            drops: drops.clone(),
            replacement_drops: None,
        },
    );
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn task_payload_destructor_does_not_kill_worker() {
    let drops = Arc::new(AtomicUsize::new(0));
    assert_worker_recovers_from_payload(
        Pool::Task(Task::new(run_job)),
        PanickingPayload {
            drops: drops.clone(),
            replacement_drops: None,
        },
    );
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn replacement_panic_payload_is_not_dropped() {
    for pool in Pool::both() {
        let drops = Arc::new(AtomicUsize::new(0));
        let replacement_drops = Arc::new(AtomicUsize::new(0));
        assert_worker_recovers_from_payload(
            pool,
            PanickingPayload {
                drops: drops.clone(),
                replacement_drops: Some(replacement_drops.clone()),
            },
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert_eq!(replacement_drops.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn ordinary_panic_payload_is_dropped_once() {
    for pool in Pool::both() {
        let drops = Arc::new(AtomicUsize::new(0));
        assert_worker_recovers_from_payload(pool, DropCounter(drops.clone()));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}

struct JoinPayload {
    other_worker_finished: Arc<AtomicBool>,
    drops: Arc<AtomicUsize>,
}

impl Drop for JoinPayload {
    fn drop(&mut self) {
        assert!(self.other_worker_finished.load(Ordering::Relaxed));
        self.drops.fetch_add(1, Ordering::Relaxed);
        panic!("joined panic payload destructor");
    }
}

#[test]
fn joined_payloads_are_disposed_after_all_workers_exit() {
    for shrink in [false, true] {
        let other_worker_finished = Arc::new(AtomicBool::new(false));
        let drops = Arc::new(AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let payload = JoinPayload {
                other_worker_finished: other_worker_finished.clone(),
                drops: drops.clone(),
            };
            workers.push(Worker {
                retire: Arc::new(AtomicBool::new(false)),
                handle: thread::spawn(move || panic::panic_any(payload)),
            });
        }
        let (job, gate) = blocked_job();
        workers.push(Worker {
            retire: Arc::new(AtomicBool::new(false)),
            handle: thread::spawn(move || {
                job();
                other_worker_finished.store(true, Ordering::Relaxed);
            }),
        });
        gate.wait_started();

        let (finished, handle) = in_background(move || {
            if shrink {
                Worker::shrink(&mut workers, 0);
                assert!(workers.is_empty());
            } else {
                Worker::join_all(workers);
            }
        });
        assert_blocked(&finished);
        gate.release();
        finished.recv_timeout(WAIT).expect("joining did not finish");
        handle.join().unwrap();
        assert_eq!(drops.load(Ordering::Relaxed), 2);
    }
}

struct ShutdownOutput {
    index: usize,
    drops: Arc<Vec<AtomicUsize>>,
    dropped: Option<mpsc::Sender<()>>,
    panics: bool,
    payload: Option<PanickingPayload>,
}

impl ShutdownOutput {
    fn new(index: usize, drops: Arc<Vec<AtomicUsize>>) -> Self {
        Self {
            index,
            drops,
            dropped: None,
            panics: false,
            payload: None,
        }
    }
}

impl Drop for ShutdownOutput {
    fn drop(&mut self) {
        self.drops[self.index].fetch_add(1, Ordering::Relaxed);
        if let Some(dropped) = &self.dropped {
            let _ = dropped.send(());
        }
        if let Some(payload) = self.payload.take() {
            panic::panic_any(payload);
        }
        if self.panics {
            panic!("output destructor panic");
        }
    }
}

type OutputJob = Box<dyn FnOnce() -> ShutdownOutput + Send + UnwindSafe>;
type OutputTask = Task<OutputJob, ShutdownOutput, fn(OutputJob) -> ShutdownOutput>;

fn run_output_job(job: OutputJob) -> ShutdownOutput {
    job()
}

fn join_outputs(task: OutputTask, as_stream: bool) {
    if as_stream {
        task.into_stream().join();
    } else {
        task.join();
    }
}

fn assert_buffered_output_panic_waits_for_worker(as_stream: bool) {
    let task = Task::with_scale(1, run_output_job);
    let drops = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let (dropped_tx, dropped_rx) = mpsc::channel();
    let mut buffered = ShutdownOutput::new(0, drops.clone());
    buffered.panics = true;
    buffered.dropped = Some(dropped_tx);
    task.send(Box::new(move || buffered));

    let (job, gate) = blocked_job();
    let last = ShutdownOutput::new(1, drops.clone());
    task.send(Box::new(move || {
        job();
        last
    }));
    // The first output has been sent before the second job starts.
    gate.wait_started();

    let (finished, handle) = in_background(move || join_outputs(task, as_stream));
    dropped_rx
        .recv_timeout(WAIT)
        .expect("buffered output was not discarded");
    assert_blocked(&finished);
    gate.release();
    finished.recv_timeout(WAIT).expect("join did not finish");
    handle.join().unwrap();
    for count in drops.iter() {
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn task_join_waits_after_buffered_output_destructor_panics() {
    assert_buffered_output_panic_waits_for_worker(false);
}

#[test]
fn stream_join_waits_after_buffered_output_destructor_panics() {
    assert_buffered_output_panic_waits_for_worker(true);
}

#[test]
fn joining_discards_each_panicking_output_once_and_finishes_queued_inputs() {
    for as_stream in [false, true] {
        let task = Task::with_scale(1, run_output_job);
        let drops = Arc::new((0..5).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let (dropped_tx, dropped_rx) = mpsc::channel();
        for index in 0..3 {
            let mut output = ShutdownOutput::new(index, drops.clone());
            output.panics = true;
            if index == 0 {
                output.dropped = Some(dropped_tx.clone());
            }
            task.send(Box::new(move || output));
        }
        let (job, gate) = blocked_job();
        let normal = ShutdownOutput::new(3, drops.clone());
        task.send(Box::new(move || {
            job();
            normal
        }));
        let mut queued = ShutdownOutput::new(4, drops.clone());
        queued.panics = true;
        let executed = Arc::new(AtomicBool::new(false));
        let observed = executed.clone();
        task.send(Box::new(move || {
            observed.store(true, Ordering::Relaxed);
            queued
        }));
        gate.wait_started();

        let (finished, handle) = in_background(move || join_outputs(task, as_stream));
        dropped_rx.recv_timeout(WAIT).unwrap();
        assert_blocked(&finished);
        gate.release();
        finished.recv_timeout(WAIT).expect("join did not finish");
        handle.join().unwrap();
        assert!(executed.load(Ordering::Relaxed));
        for count in drops.iter() {
            assert_eq!(count.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn joining_contains_panics_from_output_cleanup_payloads() {
    for as_stream in [false, true] {
        let task = Task::with_scale(1, run_output_job);
        let drops = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let replacement_drops = Arc::new(AtomicUsize::new(0));
        let mut output = ShutdownOutput::new(0, drops.clone());
        output.payload = Some(PanickingPayload {
            drops: payload_drops.clone(),
            replacement_drops: Some(replacement_drops.clone()),
        });
        task.send(Box::new(move || output));
        let normal = ShutdownOutput::new(1, drops.clone());
        task.send(Box::new(move || normal));

        let (finished, handle) = in_background(move || join_outputs(task, as_stream));
        finished.recv_timeout(WAIT).expect("join did not finish");
        handle.join().unwrap();
        assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
        assert_eq!(replacement_drops.load(Ordering::Relaxed), 0);
        for count in drops.iter() {
            assert_eq!(count.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn empty_tasks_and_streams_join_without_hanging() {
    for as_stream in [false, true] {
        for workers in [0, 1, 3] {
            let task = Task::with_scale(workers, run_output_job);
            let (finished, handle) = in_background(move || join_outputs(task, as_stream));
            finished
                .recv_timeout(WAIT)
                .expect("empty join did not finish");
            handle.join().unwrap();
        }
    }
}

fn assert_dropping_consumer_preserves_queued_work(first: ShutdownOutput, as_stream: bool) {
    let task = Task::with_scale(1, run_output_job);
    let drops = first.drops.clone();
    let (executed_tx, executed_rx) = mpsc::channel();
    let (job, gate) = blocked_job();
    let first_executed = executed_tx.clone();
    task.send(Box::new(move || {
        first_executed.send(thread::current().id()).unwrap();
        job();
        first
    }));

    let (last_dropped_tx, last_dropped_rx) = mpsc::channel();
    let mut last = ShutdownOutput::new(1, drops.clone());
    last.dropped = Some(last_dropped_tx);
    task.send(Box::new(move || {
        executed_tx.send(thread::current().id()).unwrap();
        last
    }));
    gate.wait_started();
    // There are no buffered outputs, and the receiver disconnects before the first send.
    if as_stream {
        drop(task.into_stream());
    } else {
        drop(task);
    }
    gate.release();

    let worker = executed_rx.recv_timeout(WAIT).unwrap();
    assert_eq!(
        executed_rx
            .recv_timeout(WAIT)
            .expect("queued input did not execute"),
        worker
    );
    last_dropped_rx
        .recv_timeout(WAIT)
        .expect("last output was not discarded");
    for count in drops.iter() {
        assert_eq!(count.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn task_disconnect_recovers_from_output_destructor_panic() {
    let drops = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut first = ShutdownOutput::new(0, drops);
    first.panics = true;
    assert_dropping_consumer_preserves_queued_work(first, false);
}

#[test]
fn stream_disconnect_recovers_from_output_destructor_panic() {
    let drops = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let mut first = ShutdownOutput::new(0, drops);
    first.panics = true;
    assert_dropping_consumer_preserves_queued_work(first, true);
}

#[test]
fn disconnected_outputs_are_dropped_once() {
    for as_stream in [false, true] {
        let drops = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        assert_dropping_consumer_preserves_queued_work(ShutdownOutput::new(0, drops), as_stream);
    }
}

#[test]
fn disconnected_output_cleanup_contains_nested_payload_panics() {
    for as_stream in [false, true] {
        let drops = Arc::new((0..2).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let replacement_drops = Arc::new(AtomicUsize::new(0));
        let mut first = ShutdownOutput::new(0, drops);
        first.payload = Some(PanickingPayload {
            drops: payload_drops.clone(),
            replacement_drops: Some(replacement_drops.clone()),
        });
        assert_dropping_consumer_preserves_queued_work(first, as_stream);
        assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
        assert_eq!(replacement_drops.load(Ordering::Relaxed), 0);
    }
}
