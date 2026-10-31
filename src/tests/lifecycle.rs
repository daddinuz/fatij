use super::support::*;
use crate::*;
use std::cell::RefCell;
use std::sync::Barrier;
use std::sync::mpsc;

pool_tests!(concurrent_submission_executes_each_owned_job_once, |pool| {
    bounded(move || {
        let mut pool = pool;
        pool.scale(4);
        let (completed_tx, completed_rx) = mpsc::channel();
        // No Clone implementation: executing the job must consume its captured value.
        struct OwnedJob(usize);
        thread::scope(|scope| {
            for producer in 0..4 {
                let pool = &pool;
                let completed = completed_tx.clone();
                scope.spawn(move || {
                    for offset in 0..64 {
                        let value = OwnedJob(producer * 64 + offset);
                        let completed = completed.clone();
                        pool.execute(move || completed.send(value).unwrap());
                    }
                });
            }
        });
        pool.join();
        drop(completed_tx);
        let mut completed: Vec<_> = completed_rx.into_iter().map(|value| value.0).collect();
        completed.sort_unstable();
        assert_eq!(completed, (0..256).collect::<Vec<_>>());
    });
});

#[test]
fn concurrent_producers_and_consumers_deliver_each_output_once() {
    bounded(|| {
        const PRODUCERS: usize = 4;
        const CONSUMERS: usize = 4;
        const PER_PRODUCER: usize = 256;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;

        struct Input(usize);

        let task = Task::with_scale_and_context(4, AtomicUsize::new(0), |calls, input: Input| {
            // RefCell is Send but not Sync: outputs only need to move between threads.
            RefCell::new((input.0, calls.fetch_add(1, Ordering::Relaxed)))
        });
        let ready = Barrier::new(PRODUCERS + CONSUMERS);
        let (mut submissions, mut outputs) = thread::scope(|scope| {
            let task = &task;
            let ready = &ready;
            let mut consumers = Vec::new();
            for _ in 0..CONSUMERS {
                consumers.push(scope.spawn(move || {
                    ready.wait();
                    (0..TOTAL / CONSUMERS)
                        .map(|_| {
                            let (id, result) = task
                                .recv_timeout(WAIT)
                                .expect("consumer did not receive an output");
                            let (value, ordinal) = result.unwrap().into_inner();
                            (id, value, ordinal)
                        })
                        .collect::<Vec<_>>()
                }));
            }
            let mut producers = Vec::new();
            for producer in 0..PRODUCERS {
                producers.push(scope.spawn(move || {
                    ready.wait();
                    (0..PER_PRODUCER)
                        .map(|offset| {
                            let value = producer * PER_PRODUCER + offset;
                            (task.send(Input(value)), value)
                        })
                        .collect::<Vec<_>>()
                }));
            }
            let submissions = producers
                .into_iter()
                .flat_map(|producer| producer.join().unwrap())
                .collect::<Vec<_>>();
            let outputs = consumers
                .into_iter()
                .flat_map(|consumer| consumer.join().unwrap())
                .collect::<Vec<_>>();
            (submissions, outputs)
        });
        // All producers and consumers have finished; no extra outputs may remain.
        assert!(task.try_recv().is_none());
        task.join();

        outputs.sort_unstable_by_key(|output| output.0);
        submissions.sort_unstable_by_key(|submission| submission.0);
        assert!(submissions.windows(2).all(|pair| pair[0].0 != pair[1].0));
        assert_eq!(
            outputs
                .iter()
                .map(|output| (output.0, output.1))
                .collect::<Vec<_>>(),
            submissions,
        );
        outputs.sort_unstable_by_key(|output| output.1);
        assert_eq!(
            outputs.iter().map(|output| output.1).collect::<Vec<_>>(),
            (0..TOTAL).collect::<Vec<_>>()
        );
        outputs.sort_unstable_by_key(|output| output.2);
        assert_eq!(
            outputs.iter().map(|output| output.2).collect::<Vec<_>>(),
            (0..TOTAL).collect::<Vec<_>>()
        );
    });
}

pool_tests!(
    joining_without_workers_drops_jobs_without_executing_them,
    |pool| {
        let drops = Arc::new(AtomicUsize::new(0));
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let owned = DropCounter(drops.clone());
        pool.execute(move || {
            observed.store(true, Ordering::Relaxed);
            drop(owned);
        });
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        pool.join();
        assert!(!ran.load(Ordering::Relaxed));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
);

#[test]
fn streaming_without_workers_reports_discarded_inputs_and_ends() {
    bounded(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let ran = Arc::new(AtomicBool::new(false));
        let observed = ran.clone();
        let task = Task::new(move_owned_job);
        let owned = DropCounter(drops.clone());
        let id = task.send((observed, owned));
        let stream = task.into_stream();
        assert_eq!(stream.recv(), Some((id, Err(TaskError::Discarded))));
        assert_eq!(stream.recv(), None);
        stream.join();
        assert!(!ran.load(Ordering::Relaxed));
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    });

    fn move_owned_job((ran, owned): (Arc<AtomicBool>, DropCounter)) {
        ran.store(true, Ordering::Relaxed);
        drop(owned);
    }
}

struct ThreadCleanup(Option<Box<dyn FnOnce()>>);

impl Drop for ThreadCleanup {
    fn drop(&mut self) {
        if let Some(cleanup) = self.0.take() {
            cleanup();
        }
    }
}

fn blocked_cleanup() -> (impl Job, Gate) {
    let (started_tx, started) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    // Failed assertions can drop the controller; a TLS destructor must not panic then.
    let cleanup = move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv_timeout(WAIT);
    };
    (cleanup, Gate { started, release })
}

thread_local! {
    static CLEANUP: RefCell<Option<ThreadCleanup>> = const { RefCell::new(None) };
}

pool_tests!(joining_waits_for_thread_local_cleanup, |mut pool| {
    pool.scale(1);
    let (cleanup, gate) = blocked_cleanup();
    pool.execute(move || {
        CLEANUP.with(|slot| {
            *slot.borrow_mut() = Some(ThreadCleanup(Some(Box::new(cleanup))));
        });
    });
    let (finished, handle) = in_background(move || pool.join());
    // Worker output senders are gone before TLS destructors run; draining alone is insufficient.
    gate.wait_started();
    assert_blocked(&finished);
    gate.release();
    finished
        .recv_timeout(WAIT)
        .expect("join did not finish after thread cleanup");
    handle.join().unwrap();
});

#[test]
fn stream_join_waits_for_thread_local_cleanup() {
    let task = Task::with_scale(1, run_job);
    let (cleanup, gate) = blocked_cleanup();
    task.send(Box::new(move || {
        CLEANUP.with(|slot| {
            *slot.borrow_mut() = Some(ThreadCleanup(Some(Box::new(cleanup))));
        });
    }));
    let stream = task.into_stream();
    let (finished, handle) = in_background(move || stream.join());
    gate.wait_started();
    assert_blocked(&finished);
    gate.release();
    finished
        .recv_timeout(WAIT)
        .expect("stream join did not finish after thread cleanup");
    handle.join().unwrap();
}
