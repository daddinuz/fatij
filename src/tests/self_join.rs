use super::support::*;
use crate::*;
use std::sync::mpsc;
use std::sync::{Condvar, Mutex};

const SELF_JOIN_MESSAGE: &str = "cannot join the current worker thread";
type SharedOwner<T> = Arc<(Mutex<Option<T>>, Condvar)>;

fn take_owner<T>(shared: SharedOwner<T>) -> T {
    let (slot, ready) = &*shared;
    let mut stored = slot.lock().unwrap();
    while stored.is_none() {
        stored = ready.wait(stored).unwrap();
    }
    stored.take().unwrap()
}

fn give_owner<T>(shared: SharedOwner<T>, owner: T) {
    let (slot, ready) = &*shared;
    *slot.lock().unwrap() = Some(owner);
    ready.notify_one();
}

fn panic_message(result: std::thread::Result<()>) -> Option<String> {
    result.err().map(|payload| {
        if let Some(message) = payload.downcast_ref::<&str>() {
            (*message).to_owned()
        } else if let Some(message) = payload.downcast_ref::<String>() {
            message.clone()
        } else {
            String::from("unexpected panic payload")
        }
    })
}

fn assert_self_join_rejected<T: Send + 'static>(
    create: impl FnOnce(Box<dyn Job>, &Gate, Box<dyn Job>) -> T,
    join: fn(T),
) {
    let shared: SharedOwner<T> = Arc::new((Mutex::new(None), Condvar::new()));
    let received = shared.clone();
    let (rejected_tx, rejected_rx) = mpsc::channel();
    let join_job = Box::new(move || {
        let owner = take_owner(received);
        let result = panic::catch_unwind(AssertUnwindSafe(|| join(owner)));
        let _ = rejected_tx.send(panic_message(result));
    });
    let (blocked, gate) = blocked_job();
    // A separate worker remains blocked: rejection must precede waiting for any worker.
    let owner = create(Box::new(blocked), &gate, join_job);
    give_owner(shared, owner);
    let message = rejected_rx
        .recv_timeout(WAIT)
        .expect("self-join did not reject before waiting");
    gate.release();
    assert_eq!(message.as_deref(), Some(SELF_JOIN_MESSAGE));
}

pool_tests!(rejects_self_join, |mut pool| {
    assert_self_join_rejected(
        |blocked, gate, join_job| {
            pool.scale(2);
            pool.execute(blocked);
            gate.wait_started();
            pool.execute(join_job);
            pool
        },
        Pool::join,
    );
});

#[test]
fn stream_rejects_self_join() {
    assert_self_join_rejected(
        |blocked, gate, join_job| {
            let task = Task::with_scale(2, run_job);
            task.send(blocked);
            gate.wait_started();
            task.send(join_job);
            task.into_stream()
        },
        Stream::join,
    );
}

pool_tests!(
    self_retirement_preserves_workers_and_queued_work,
    |mut pool| {
        pool.scale(1);
        let shared: SharedOwner<Pool> = Arc::new((Mutex::new(None), Condvar::new()));
        let received = shared.clone();
        let (returned_tx, returned_rx) = mpsc::channel();
        let (executed_tx, executed_rx) = mpsc::channel();
        pool.execute(move || {
            let mut owner = take_owner(received);
            let result = panic::catch_unwind(AssertUnwindSafe(|| owner.scale(0)));
            let message = panic_message(result);
            let worker = thread::current().id();
            owner.execute(move || {
                let _ = executed_tx.send(thread::current().id());
            });
            let _ = returned_tx.send((owner, message, worker));
        });
        give_owner(shared, pool);

        let (owner, message, worker) = returned_rx
            .recv_timeout(WAIT)
            .expect("self-retirement did not return the owner");
        // Join from the test thread so all worker cleanup completes before assertions.
        owner.join();
        assert_eq!(message.as_deref(), Some(SELF_JOIN_MESSAGE));
        assert_eq!(executed_rx.recv_timeout(WAIT).unwrap(), worker);
    }
);

pool_tests!(worker_can_resize_without_retiring_itself, |mut pool| {
    pool.scale(1);
    let shared: SharedOwner<Pool> = Arc::new((Mutex::new(None), Condvar::new()));
    let received = shared.clone();
    let (returned_tx, returned_rx) = mpsc::channel();
    let (executed_tx, executed_rx) = mpsc::channel();
    pool.execute(move || {
        let mut owner = take_owner(received);
        owner.scale(1); // No change.
        owner.scale(2); // Grow from a worker.
        owner.scale(1); // The original worker is retained; only the new worker retires.
        let worker = thread::current().id();
        owner.execute(move || {
            let _ = executed_tx.send(thread::current().id());
        });
        let _ = returned_tx.send((owner, worker));
    });
    give_owner(shared, pool);

    let (owner, worker) = returned_rx
        .recv_timeout(WAIT)
        .expect("allowed resizing did not return the owner");
    owner.join();
    assert_eq!(executed_rx.recv_timeout(WAIT).unwrap(), worker);
});

pool_tests!(worker_can_join_another_pool, |mut pool| {
    pool.scale(1);
    let other = ThreadPool::with_scale(1);
    let (completed_tx, completed_rx) = mpsc::channel();
    other.execute(move || completed_tx.send(()).unwrap());
    let (joined_tx, joined_rx) = mpsc::channel();
    // JoinHandle is not UnwindSafe; transfer ownership through a mutex into the job.
    let other = Mutex::new(Some(other));
    pool.execute(move || {
        let other = other.into_inner().unwrap().unwrap();
        other.join();
        let _ = joined_tx.send(());
    });
    bounded(move || pool.join());
    joined_rx
        .recv_timeout(WAIT)
        .expect("joining another pool was rejected");
    completed_rx.recv_timeout(WAIT).unwrap();
});
