use super::support::*;
use crate::*;
use std::sync::mpsc;

pool_tests!(shrinking_waits_for_running_jobs, |mut pool| {
    pool.scale(1);
    let (job, gate) = blocked_job();
    pool.execute(job);
    gate.wait_started();

    let (finished, handle) = in_background(move || {
        pool.scale(0);
        pool
    });
    assert_blocked(&finished);
    gate.release();
    let pool = finished.recv_timeout(WAIT).expect("shrink did not finish");
    handle.join().unwrap();
    pool.join();
});

pool_tests!(idle_workers_can_be_retired, |mut pool| {
    pool.scale(4);
    let (finished, handle) = in_background(move || {
        pool.scale(0);
        pool.join();
    });
    finished
        .recv_timeout(WAIT)
        .expect("idle workers did not stop");
    handle.join().unwrap();
});

pool_tests!(
    shrinking_and_growing_does_not_leave_extra_workers,
    |mut pool| {
        pool.scale(1);
        for _ in 0..3 {
            pool.scale(0);
            pool.scale(1);
        }

        let (first, first_gate) = blocked_job();
        let (second, second_gate) = blocked_job();
        pool.execute(first);
        first_gate.wait_started();
        pool.execute(second);
        assert_blocked(&second_gate.started);
        first_gate.release();
        second_gate.wait_started();
        second_gate.release();
        pool.join();
    }
);

pool_tests!(
    queued_jobs_survive_shrinking_and_run_once_after_growing,
    |mut pool| {
        pool.scale(1);
        let (job, gate) = blocked_job();
        pool.execute(job);
        gate.wait_started();
        let (completed_tx, completed_rx) = mpsc::channel();
        for index in 0..32 {
            let completed = completed_tx.clone();
            pool.execute(move || completed.send(index).unwrap());
        }
        // Keep a real backlog queued while the only worker is asked to retire.
        let (finished, handle) = in_background(move || {
            pool.scale(0);
            pool
        });
        assert_blocked(&finished);
        gate.release();
        let mut pool = finished.recv_timeout(WAIT).expect("shrink did not finish");
        handle.join().unwrap();
        let mut completed: Vec<_> = completed_rx.try_iter().collect();

        for index in 32..64 {
            let completed = completed_tx.clone();
            pool.execute(move || completed.send(index).unwrap());
        }
        assert!(matches!(
            completed_rx.try_recv(),
            Err(mpsc::TryRecvError::Empty)
        ));

        pool.scale(2);
        pool.join();
        drop(completed_tx);
        completed.extend(completed_rx.try_iter());
        completed.sort_unstable();
        assert_eq!(completed, (0..64).collect::<Vec<_>>());
    }
);

pool_tests!(
    shrinking_to_nonzero_keeps_the_requested_number_of_workers,
    |mut pool| {
        pool.scale(4);
        pool.scale(2);
        let (first, first_gate) = blocked_job();
        let (second, second_gate) = blocked_job();
        let (third, third_gate) = blocked_job();
        pool.execute(first);
        pool.execute(second);
        first_gate.wait_started();
        second_gate.wait_started();
        pool.execute(third);
        assert_blocked(&third_gate.started);
        first_gate.release();
        third_gate.wait_started();
        second_gate.release();
        third_gate.release();
        pool.join();
    }
);

pool_tests!(
    join_waits_for_remaining_workers_after_shrinking,
    |mut pool| {
        pool.scale(3);
        pool.scale(1);
        let (job, gate) = blocked_job();
        pool.execute(job);
        gate.wait_started();

        let (finished, handle) = in_background(move || pool.join());
        assert_blocked(&finished);
        gate.release();
        finished.recv_timeout(WAIT).expect("join did not finish");
        handle.join().unwrap();
    }
);

pool_tests!(
    ordinary_panics_do_not_prevent_draining_jobs_after_resizing,
    |mut pool| {
        pool.scale(2);
        pool.scale(1);
        pool.execute(|| panic!("job panic"));
        let (completed_tx, completed_rx) = mpsc::channel();
        pool.execute(move || completed_tx.send(()).unwrap());
        pool.join();
        completed_rx.recv_timeout(WAIT).unwrap();
    }
);

#[test]
fn task_stream_drains_outputs_and_disconnects_after_resizing() {
    let mut task = Task::with_scale_and_context(3, 7, |context, input| context + input);
    task.scale(1);
    task.scale(0);
    for input in 0..32 {
        task.send(input);
    }
    task.scale(2);

    let (finished, handle) = in_background(move || {
        let stream = task.into_stream();
        let mut outputs: Vec<_> = stream.iter().map(successful).collect();
        outputs.sort_unstable();
        stream.join();
        outputs
    });
    let outputs = finished
        .recv_timeout(WAIT)
        .expect("stream did not disconnect");
    handle.join().unwrap();
    assert_eq!(outputs, (7..39).collect::<Vec<_>>());
}

#[test]
fn stream_join_waits_for_remaining_workers_after_shrinking() {
    let mut task = Task::with_scale(3, run_job);
    task.scale(1);
    let (job, gate) = blocked_job();
    task.send(Box::new(job));
    gate.wait_started();

    let stream = task.into_stream();
    let (finished, handle) = in_background(move || stream.join());
    assert_blocked(&finished);
    gate.release();
    finished
        .recv_timeout(WAIT)
        .expect("stream join did not finish");
    handle.join().unwrap();
}
