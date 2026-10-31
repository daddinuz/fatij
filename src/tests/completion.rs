use super::support::*;
use crate::*;

fn buffered_panics() -> (TestTask, Gate, Vec<TaskOutput<()>>) {
    let task = Task::with_scale(1, run_job);
    let mut expected = Vec::new();
    for index in 0..7 {
        let message = format!("input {index} failed");
        let payload = message.clone();
        let id = task.send(Box::new(move || panic::panic_any(payload)));
        expected.push((id, Err(TaskError::Panicked(Some(message)))));
    }
    let (job, gate) = blocked_job();
    let id = task.send(Box::new(job));
    expected.push((id, Ok(())));
    // Starting the last job proves the worker has sent every earlier failure.
    gate.wait_started();
    (task, gate, expected)
}

#[test]
fn task_receive_methods_preserve_panic_ids_and_messages() {
    bounded(|| {
        let (task, gate, expected) = buffered_panics();
        assert_eq!(task.recv(), expected[0]);
        assert_eq!(task.recv_timeout(Duration::ZERO), Some(expected[1].clone()));
        assert_eq!(
            task.recv_deadline(Instant::now()),
            Some(expected[2].clone())
        );
        assert_eq!(task.try_recv(), Some(expected[3].clone()));
        assert_eq!(task.try_iter().next(), Some(expected[4].clone()));
        assert_eq!(task.iter().take(2).collect::<Vec<_>>(), expected[5..7]);
        assert_eq!(task.try_recv(), None);
        gate.release();
        assert_eq!(task.recv(), expected[7]);
        task.join();
    });
}

#[test]
fn stream_receive_methods_preserve_panic_ids_and_messages() {
    bounded(|| {
        let (task, gate, expected) = buffered_panics();
        let stream = task.into_stream();
        assert_eq!(stream.recv(), Some(expected[0].clone()));
        assert_eq!(stream.recv_timeout(Duration::ZERO), Ok(expected[1].clone()));
        assert_eq!(
            stream.recv_deadline(Instant::now()),
            Ok(expected[2].clone())
        );
        assert_eq!(stream.try_recv(), Ok(expected[3].clone()));
        assert_eq!(stream.try_iter().next(), Some(expected[4].clone()));
        assert_eq!(stream.iter().take(2).collect::<Vec<_>>(), expected[5..7]);
        assert_eq!(stream.try_recv(), Err(TryRecvError::Empty));
        gate.release();
        assert_eq!(stream.recv(), Some(expected[7].clone()));
        assert_eq!(stream.recv(), None);
        stream.join();
    });
}

#[test]
fn stream_receive_methods_preserve_discarded_ids() {
    bounded(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let task = Task::new(|_: DropCounter| ());
        let expected: Vec<_> = (0..7)
            .map(|_| {
                let id = task.send(DropCounter(drops.clone()));
                (id, Err(TaskError::Discarded))
            })
            .collect();
        let stream = task.into_stream();
        assert_eq!(drops.load(Ordering::Relaxed), 7);
        assert_eq!(stream.recv(), Some(expected[0].clone()));
        assert_eq!(stream.recv_timeout(Duration::ZERO), Ok(expected[1].clone()));
        assert_eq!(
            stream.recv_deadline(Instant::now()),
            Ok(expected[2].clone())
        );
        assert_eq!(stream.try_recv(), Ok(expected[3].clone()));
        assert_eq!(stream.try_iter().next(), Some(expected[4].clone()));
        assert_eq!(stream.iter().collect::<Vec<_>>(), expected[5..]);
        assert_eq!(stream.recv(), None);
        assert_eq!(
            stream.recv_timeout(Duration::ZERO),
            Err(RecvTimeoutError::Disconnected)
        );
        assert_eq!(
            stream.recv_deadline(Instant::now()),
            Err(RecvTimeoutError::Disconnected)
        );
        assert_eq!(stream.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(stream.try_iter().next(), None);
        assert_eq!(stream.iter().next(), None);
        stream.join();
    });
}

struct DiscardInput {
    index: usize,
    drops: Arc<Vec<AtomicUsize>>,
    panics: bool,
    payload: Option<PanickingPayload>,
}

impl Drop for DiscardInput {
    fn drop(&mut self) {
        self.drops[self.index].fetch_add(1, Ordering::Relaxed);
        if let Some(payload) = self.payload.take() {
            panic::panic_any(payload);
        }
        if self.panics {
            panic!("discarded input destructor panic");
        }
    }
}

#[test]
fn zero_worker_finalization_contains_input_and_payload_destructor_panics() {
    bounded(|| {
        for as_stream in [false, true] {
            let drops = Arc::new((0..4).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
            let payload_drops = Arc::new(AtomicUsize::new(0));
            let replacement_drops = Arc::new(AtomicUsize::new(0));
            let ran = Arc::new(AtomicBool::new(false));
            let task = Task::with_context(ran.clone(), |ran, input: DiscardInput| {
                ran.store(true, Ordering::Relaxed);
                input.index
            });
            let ids: Vec<_> = (0..4)
                .map(|index| {
                    task.send(DiscardInput {
                        index,
                        drops: drops.clone(),
                        panics: index == 1,
                        payload: (index == 2).then(|| PanickingPayload {
                            drops: payload_drops.clone(),
                            replacement_drops: Some(replacement_drops.clone()),
                        }),
                    })
                })
                .collect();
            if as_stream {
                let stream = task.into_stream();
                assert_eq!(
                    stream.iter().collect::<Vec<_>>(),
                    ids.into_iter()
                        .map(|id| (id, Err(TaskError::Discarded)))
                        .collect::<Vec<_>>()
                );
                stream.join();
            } else {
                task.join();
            }
            assert!(!ran.load(Ordering::Relaxed));
            assert!(drops.iter().all(|drops| drops.load(Ordering::Relaxed) == 1));
            assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
            assert_eq!(replacement_drops.load(Ordering::Relaxed), 0);
        }
    });
}

#[test]
fn out_of_order_completions_keep_the_submitted_ids() {
    bounded(|| {
        let task = Task::with_scale(2, run_job);
        let (job, gate) = blocked_job();
        let slow = task.send(Box::new(job));
        gate.wait_started();
        let fast = task.send(Box::new(|| ()));
        assert_ne!(slow, fast);
        assert_eq!(task.recv_timeout(WAIT), Some((fast, Ok(()))));
        gate.release();
        let stream = task.into_stream();
        assert_eq!(stream.recv(), Some((slow, Ok(()))));
        assert_eq!(stream.recv(), None);
        stream.join();
    });
}

#[test]
fn ids_survive_pausing_resuming_and_finalizing_at_zero() {
    bounded(|| {
        let mut task = Task::new(|value: usize| value * 2);
        let first = task.send(1);
        let second = task.send(2);
        task.scale(2);
        let mut first_batch = vec![task.recv(), task.recv()];
        first_batch.sort_unstable_by_key(|output| output.0);
        assert_eq!(first_batch, vec![(first, Ok(2)), (second, Ok(4))]);
        task.scale(0);
        let resumed = task.send(3);
        assert_eq!(task.try_recv(), None);
        task.scale(1);
        assert_eq!(task.recv(), (resumed, Ok(6)));
        task.scale(0);
        let discarded = task.send(4);
        assert_ne!(discarded, resumed);
        let stream = task.into_stream();
        assert_eq!(stream.recv(), Some((discarded, Err(TaskError::Discarded))));
        assert_eq!(stream.recv(), None);
        stream.join();
    });
}

#[test]
fn finalizing_at_zero_keeps_buffered_successes_and_reports_pending_inputs() {
    bounded(|| {
        let task = Task::with_scale(1, run_job);
        let completed = task.send(Box::new(|| ()));
        let (job, gate) = blocked_job();
        let running = task.send(Box::new(job));
        gate.wait_started();
        let (finished, handle) = in_background(move || {
            let mut task = task;
            task.scale(0);
            task
        });
        assert_blocked(&finished);
        gate.release();
        let task = finished.recv_timeout(WAIT).unwrap();
        handle.join().unwrap();
        let discarded = task.send(Box::new(|| ()));
        let stream = task.into_stream();
        assert_eq!(
            stream.iter().collect::<Vec<_>>(),
            vec![
                (completed, Ok(())),
                (running, Ok(())),
                (discarded, Err(TaskError::Discarded)),
            ]
        );
        stream.join();
    });
}

#[test]
fn ordinary_function_errors_remain_successful_executor_outputs() {
    bounded(|| {
        let task = Task::with_scale(1, |()| Err::<(), _>("application error"));
        let id = task.send(());
        let stream = task.into_stream();
        assert_eq!(stream.recv(), Some((id, Ok(Err("application error")))));
        assert_eq!(stream.recv(), None);
        stream.join();
    });
}

#[test]
fn id_exhaustion_never_wraps_or_enqueues_rejected_inputs() {
    bounded(|| {
        let drops = Arc::new(AtomicUsize::new(0));
        let task = Task::new(|_: DropCounter| ());
        task.next_id.store(usize::MAX - 1, Ordering::Relaxed);
        let last = task.send(DropCounter(drops.clone()));
        for _ in 0..2 {
            assert!(
                panic::catch_unwind(AssertUnwindSafe(|| {
                    task.send(DropCounter(drops.clone()));
                }))
                .is_err()
            );
        }
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        let stream = task.into_stream();
        assert_eq!(
            stream.iter().collect::<Vec<_>>(),
            vec![(last, Err(TaskError::Discarded))]
        );
        stream.join();
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    });
}
