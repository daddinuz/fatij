use super::support::*;
use crate::*;
use std::collections::HashSet;
use std::sync::Barrier;
use std::sync::mpsc;

struct SharedContext {
    ready: Barrier,
    calls: AtomicUsize,
}

#[test]
fn non_clone_context_is_shared_by_concurrent_workers() {
    bounded(|| {
        let context = SharedContext {
            ready: Barrier::new(4),
            calls: AtomicUsize::new(0),
        };
        let task = Task::with_scale_and_context(4, context, |context, ()| {
            // Every worker must be active before any invocation can finish.
            context.ready.wait();
            (
                std::ptr::from_ref(context) as usize,
                context.calls.fetch_add(1, Ordering::Relaxed),
                thread::current().id(),
            )
        });
        for _ in 0..4 {
            task.send(());
        }
        let stream = task.into_stream();
        let outputs: Vec<_> = stream.iter().map(successful).collect();
        stream.join();

        assert_eq!(outputs.len(), 4);
        assert!(outputs.iter().all(|output| output.0 == outputs[0].0));
        let mut calls: Vec<_> = outputs.iter().map(|output| output.1).collect();
        calls.sort_unstable();
        assert_eq!(calls, vec![0, 1, 2, 3]);
        assert_eq!(
            outputs
                .iter()
                .map(|output| output.2)
                .collect::<HashSet<_>>()
                .len(),
            4
        );
    });
}

#[test]
fn shared_context_state_survives_scaling_to_zero_and_back() {
    bounded(|| {
        let mut task = Task::with_context(AtomicUsize::new(0), |sum, value: usize| {
            sum.fetch_add(value, Ordering::Relaxed) + value
        });
        for value in 1..=3 {
            task.send(value);
        }
        assert_eq!(task.try_recv(), None);
        task.scale(1);
        assert_eq!(successful(task.recv()), 1);
        assert_eq!(successful(task.recv()), 3);
        assert_eq!(successful(task.recv()), 6);

        task.scale(0);
        task.send(4);
        task.send(5);
        assert_eq!(task.try_recv(), None);
        task.scale(2);
        let stream = task.into_stream();
        let outputs: Vec<_> = stream.iter().map(successful).collect();
        stream.join();
        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs.into_iter().max(), Some(15));
    });
}

struct DropContext {
    drops: Arc<AtomicUsize>,
    dropped: mpsc::Sender<()>,
}

impl Drop for DropContext {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
        let _ = self.dropped.send(());
    }
}

fn tracked_context() -> (DropContext, Arc<AtomicUsize>, mpsc::Receiver<()>) {
    let drops = Arc::new(AtomicUsize::new(0));
    let (dropped, received) = mpsc::channel();
    (
        DropContext {
            drops: drops.clone(),
            dropped,
        },
        drops,
        received,
    )
}

#[test]
fn context_is_dropped_once_after_explicit_shutdown() {
    bounded(|| {
        for as_stream in [false, true] {
            for workers in [0, 3] {
                let (context, drops, dropped) = tracked_context();
                let mut task = Task::with_context(context, |_, value: usize| value * 2);
                task.scale(workers);
                for value in 0..8 {
                    task.send(value);
                }
                assert_eq!(drops.load(Ordering::Relaxed), 0);
                if as_stream {
                    task.into_stream().join();
                } else {
                    task.join();
                }
                dropped.recv_timeout(WAIT).unwrap();
                assert_eq!(drops.load(Ordering::Relaxed), 1);
            }
        }
    });
}

fn assert_context_outlives_consumer(as_stream: bool) {
    bounded(move || {
        let (context, drops, dropped) = tracked_context();
        let task = Task::with_scale_and_context(1, context, |_, job: Box<dyn Job>| job());
        let (job, gate) = blocked_job();
        task.send(Box::new(job));
        gate.wait_started();
        if as_stream {
            let stream = task.into_stream();
            assert_eq!(drops.load(Ordering::Relaxed), 0);
            drop(stream);
        } else {
            drop(task);
        }
        assert_eq!(drops.load(Ordering::Relaxed), 0);
        gate.release();
        dropped
            .recv_timeout(WAIT)
            .expect("worker did not release its context");
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    });
}

#[test]
fn shared_context_outlives_a_dropped_task() {
    assert_context_outlives_consumer(false);
}

#[test]
fn shared_context_outlives_a_dropped_stream() {
    assert_context_outlives_consumer(true);
}

struct TrackedInput {
    value: usize,
    drops: Arc<Vec<AtomicUsize>>,
}

impl Drop for TrackedInput {
    fn drop(&mut self) {
        self.drops[self.value].fetch_add(1, Ordering::Relaxed);
    }
}

struct PanicContext {
    calls: AtomicUsize,
    failed_on: mpsc::Sender<thread::ThreadId>,
    payload_drops: Option<Arc<AtomicUsize>>,
}

fn process(context: &PanicContext, input: TrackedInput) -> (usize, usize, thread::ThreadId) {
    let call = context.calls.fetch_add(1, Ordering::Relaxed) + 1;
    if input.value == 0 {
        context.failed_on.send(thread::current().id()).unwrap();
        if let Some(drops) = &context.payload_drops {
            panic::panic_any(PanickingPayload {
                drops: drops.clone(),
                replacement_drops: None,
            });
        }
        panic!("task function panic");
    }
    (input.value * 2, call, thread::current().id())
}

fn assert_context_reused_after_panic(payload_drops: Option<Arc<AtomicUsize>>) {
    bounded(move || {
        let drops = Arc::new((0..3).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
        let (failed_on, failed_rx) = mpsc::channel();
        let message = payload_drops
            .is_none()
            .then(|| "task function panic".to_owned());
        let context = PanicContext {
            calls: AtomicUsize::new(0),
            failed_on,
            payload_drops,
        };
        let task = Task::with_scale_and_context(1, context, process);
        let ids: Vec<_> = (0..3)
            .map(|value| {
                task.send(TrackedInput {
                    value,
                    drops: drops.clone(),
                })
            })
            .collect();
        let stream = task.into_stream();
        let worker = failed_rx.recv_timeout(WAIT).expect("input did not panic");
        assert_eq!(
            stream.recv(),
            Some((ids[0], Err(TaskError::Panicked(message))))
        );
        assert_eq!(stream.recv(), Some((ids[1], Ok((2, 2, worker)))));
        assert_eq!(stream.recv(), Some((ids[2], Ok((4, 3, worker)))));
        assert_eq!(stream.recv(), None);
        stream.join();
        for count in drops.iter() {
            assert_eq!(count.load(Ordering::Relaxed), 1);
        }
    });
}

#[test]
fn function_panic_preserves_shared_context_and_drops_inputs_once() {
    assert_context_reused_after_panic(None);
}

#[test]
fn payload_cleanup_preserves_shared_context_and_stream_completion() {
    let payload_drops = Arc::new(AtomicUsize::new(0));
    assert_context_reused_after_panic(Some(payload_drops.clone()));
    assert_eq!(payload_drops.load(Ordering::Relaxed), 1);
}

#[test]
fn context_does_not_require_unwind_safe() {
    // &mut usize is Send + Sync + RefUnwindSafe but not UnwindSafe. Check the
    // constructor's type without allocating a leaked value to obtain a static reference.
    fn construct(context: &'static mut usize) -> Task<usize, usize, &'static mut usize> {
        Task::with_context(context, |context, input| **context + input)
    }
    let _constructor = construct;
}
