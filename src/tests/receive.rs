use super::support::*;
use crate::*;

type NumberJob = Box<dyn FnOnce() -> u32 + Send + UnwindSafe>;
type NumberTask = Task<NumberJob, u32, fn(NumberJob) -> u32>;

const LAST: u32 = 99;

fn run_number_job(job: NumberJob) -> u32 {
    job()
}

// With one worker, reaching the gated last job proves earlier outputs were sent.
fn buffered_task(values: &[u32]) -> (NumberTask, Gate, Vec<TaskId>) {
    let task = Task::with_scale(1, run_number_job);
    let mut ids = Vec::new();
    for &value in values {
        ids.push(task.send(Box::new(move || value)));
    }
    let (job, gate) = blocked_job();
    ids.push(task.send(Box::new(move || {
        job();
        LAST
    })));
    gate.wait_started();
    (task, gate, ids)
}

#[test]
fn task_receive_methods_consume_buffered_outputs_and_leave_pending_work() {
    bounded(|| {
        let (task, gate, ids) = buffered_task(&[10, 20, 30, 40]);
        assert_eq!(task.recv(), (ids[0], Ok(10)));
        assert_eq!(task.recv_timeout(Duration::ZERO), Some((ids[1], Ok(20))));
        assert_eq!(task.recv_deadline(Instant::now()), Some((ids[2], Ok(30))));
        assert_eq!(task.try_recv(), Some((ids[3], Ok(40))));

        assert_eq!(task.try_recv(), None);
        assert_eq!(task.recv_timeout(Duration::ZERO), None);
        assert_eq!(task.recv_deadline(Instant::now()), None);
        assert_eq!(task.try_iter().next(), None);

        gate.release();
        assert_eq!(task.recv_timeout(WAIT), Some((ids[4], Ok(LAST))));
        task.join();
    });
}

#[test]
fn stream_receive_methods_distinguish_values_from_empty_and_finished_streams() {
    bounded(|| {
        let (task, gate, ids) = buffered_task(&[10, 20, 30, 40]);
        let stream = task.into_stream();
        assert_eq!(stream.recv(), Some((ids[0], Ok(10))));
        assert_eq!(stream.recv_timeout(Duration::ZERO), Ok((ids[1], Ok(20))));
        assert_eq!(stream.recv_deadline(Instant::now()), Ok((ids[2], Ok(30))));
        assert_eq!(stream.try_recv(), Ok((ids[3], Ok(40))));

        // The worker is still active, so these empty receives do not cancel it.
        assert_eq!(stream.try_recv(), Err(TryRecvError::Empty));
        assert_eq!(
            stream.recv_timeout(Duration::ZERO),
            Err(RecvTimeoutError::Timeout)
        );
        assert_eq!(
            stream.recv_deadline(Instant::now()),
            Err(RecvTimeoutError::Timeout)
        );
        assert_eq!(stream.try_iter().next(), None);

        gate.release();
        assert_eq!(stream.recv(), Some((ids[4], Ok(LAST))));
        assert_eq!(stream.recv(), None);
        assert_eq!(stream.try_recv(), Err(TryRecvError::Disconnected));
        assert_eq!(
            stream.recv_timeout(Duration::ZERO),
            Err(RecvTimeoutError::Disconnected)
        );
        assert_eq!(
            stream.recv_deadline(Instant::now()),
            Err(RecvTimeoutError::Disconnected)
        );
        assert_eq!(stream.iter().next(), None);
        stream.join();
    });
}

#[test]
fn task_try_iter_drains_ready_outputs_without_waiting_for_pending_input() {
    bounded(|| {
        let (task, gate, ids) = buffered_task(&[1, 2, 3]);
        assert_eq!(
            task.try_iter().collect::<Vec<_>>(),
            vec![(ids[0], Ok(1)), (ids[1], Ok(2)), (ids[2], Ok(3))]
        );
        assert_eq!(task.try_iter().next(), None);
        gate.release();
        assert_eq!(task.recv(), (ids[3], Ok(LAST)));
        task.join();
    });
}

#[test]
fn stream_try_iter_can_stop_before_the_stream_ends() {
    bounded(|| {
        let (task, gate, ids) = buffered_task(&[1, 2, 3]);
        let stream = task.into_stream();
        assert_eq!(
            stream.try_iter().collect::<Vec<_>>(),
            vec![(ids[0], Ok(1)), (ids[1], Ok(2)), (ids[2], Ok(3))]
        );
        assert_eq!(stream.try_iter().next(), None);
        gate.release();
        assert_eq!(stream.iter().collect::<Vec<_>>(), vec![(ids[3], Ok(LAST))]);
        stream.join();
    });
}

#[test]
fn task_iter_waits_for_the_requested_number_of_outputs() {
    let (task, gate, ids) = buffered_task(&[1, 2]);
    let (finished, handle) = in_background(move || {
        let outputs = task.iter().take(3).collect::<Vec<_>>();
        task.join();
        outputs
    });
    assert_blocked(&finished);
    gate.release();
    assert_eq!(
        finished.recv_timeout(WAIT).unwrap(),
        vec![(ids[0], Ok(1)), (ids[1], Ok(2)), (ids[2], Ok(LAST))]
    );
    handle.join().unwrap();
}

#[test]
fn stream_iter_waits_for_pending_outputs_before_ending() {
    let (task, gate, ids) = buffered_task(&[1, 2]);
    let stream = task.into_stream();
    let (finished, handle) = in_background(move || {
        let outputs = stream.iter().collect::<Vec<_>>();
        stream.join();
        outputs
    });
    assert_blocked(&finished);
    gate.release();
    assert_eq!(
        finished.recv_timeout(WAIT).unwrap(),
        vec![(ids[0], Ok(1)), (ids[1], Ok(2)), (ids[2], Ok(LAST))]
    );
    handle.join().unwrap();
}

#[derive(Clone, Copy)]
enum WaitFor {
    Timeout,
    Deadline,
}

fn assert_task_receive_waits(wait: WaitFor) {
    let (task, gate, ids) = buffered_task(&[]);
    let (finished, handle) = in_background(move || {
        let output = match wait {
            WaitFor::Timeout => task.recv_timeout(WAIT),
            WaitFor::Deadline => task.recv_deadline(Instant::now() + WAIT),
        };
        task.join();
        output
    });
    assert_blocked(&finished);
    gate.release();
    assert_eq!(
        finished.recv_timeout(WAIT).unwrap(),
        Some((ids[0], Ok(LAST)))
    );
    handle.join().unwrap();
}

fn assert_stream_receive_waits(wait: WaitFor) {
    let (task, gate, ids) = buffered_task(&[]);
    let stream = task.into_stream();
    let (finished, handle) = in_background(move || {
        let output = match wait {
            WaitFor::Timeout => stream.recv_timeout(WAIT),
            WaitFor::Deadline => stream.recv_deadline(Instant::now() + WAIT),
        };
        stream.join();
        output
    });
    assert_blocked(&finished);
    gate.release();
    assert_eq!(finished.recv_timeout(WAIT).unwrap(), Ok((ids[0], Ok(LAST))));
    handle.join().unwrap();
}

#[test]
fn task_recv_timeout_waits_for_a_future_output() {
    assert_task_receive_waits(WaitFor::Timeout);
}

#[test]
fn task_recv_deadline_waits_for_a_future_output() {
    assert_task_receive_waits(WaitFor::Deadline);
}

#[test]
fn stream_recv_timeout_waits_for_a_future_output() {
    assert_stream_receive_waits(WaitFor::Timeout);
}

#[test]
fn stream_recv_deadline_waits_for_a_future_output() {
    assert_stream_receive_waits(WaitFor::Deadline);
}
