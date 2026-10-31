#![feature(mpmc_channel)]
#![doc = include_str!("../README.md")]

use std::any::Any;
use std::fmt;
use std::panic::{self, AssertUnwindSafe, RefUnwindSafe, UnwindSafe};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpmc::{self, Receiver, SendError, Sender};
use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Debug)]
struct Channel<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

impl<T> Channel<T> {
    fn new() -> Self {
        let (sender, receiver) = mpmc::channel();
        Self { sender, receiver }
    }
}

/// An owned, one-shot function that can execute on a pool worker.
///
/// Every type implementing `FnOnce() + Send + UnwindSafe + 'static` implements
/// this trait automatically. Use a `move` closure to transfer captured values
/// into the job; non-static borrows are not supported.
pub trait Job: 'static + Send + UnwindSafe + FnOnce() {}

impl<T: 'static + Send + UnwindSafe + FnOnce()> Job for T {}

fn drop_panic_payload(payload: Box<dyn Any + Send>) {
    // The payload is consumed and never reused after unwinding.
    if let Err(replacement) = panic::catch_unwind(AssertUnwindSafe(move || drop(payload))) {
        // Dropping the replacement could panic again, so do not attempt recursive cleanup.
        std::mem::forget(replacement);
    }
}

fn drop_catching_unwind<T: UnwindSafe>(value: T) {
    if let Err(payload) = panic::catch_unwind(move || drop(value)) {
        drop_panic_payload(payload);
    }
}

/// Identifies an input submitted to a particular [`Task`].
///
/// IDs are unique within one task, including across worker resizes. IDs from
/// different tasks can compare equal. Their order does not guarantee enqueue,
/// execution, or completion order when inputs are submitted concurrently.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TaskId(usize);

/// An executor failure for a submitted input.
///
/// Errors returned normally by the task function remain part of its output
/// type `O`; they are not converted into this error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TaskError {
    /// The task function panicked and unwinding was caught.
    /// The string panic message, or `None` for a non-string payload.
    Panicked(Option<String>),
    /// The input was discarded without execution when finalized with zero workers.
    Discarded,
}

impl TaskError {
    fn from_panic(payload: Box<dyn Any + Send>) -> Self {
        let message = match payload.downcast_ref::<&str>() {
            Some(message) => Some((*message).to_owned()),
            None => payload.downcast_ref::<String>().cloned(),
        };

        drop_panic_payload(payload);
        Self::Panicked(message)
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked(Some(message)) => write!(formatter, "task panicked: {message}"),
            Self::Panicked(None) => formatter.write_str("task panicked with a non-string payload"),
            Self::Discarded => formatter.write_str("task input was discarded without execution"),
        }
    }
}

impl std::error::Error for TaskError {}

/// A submitted input's ID and its successful output or executor error.
///
/// Returned by every [`Task`] and [`Stream`] receive method and iterator.
pub type TaskOutput<O> = (TaskId, Result<O, TaskError>);

const RETIRE_POLL_INTERVAL: Duration = Duration::from_millis(20);

#[derive(Debug)]
struct Worker {
    retire: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl Worker {
    fn spawn<T: Send + 'static>(
        receiver: Receiver<T>,
        mut function: impl FnMut(T) + Send + 'static,
    ) -> Self {
        let retire = Arc::new(AtomicBool::new(false));
        let requested = retire.clone();
        let handle = thread::spawn(move || {
            while !requested.load(Ordering::Relaxed) {
                match receiver.recv_timeout(RETIRE_POLL_INTERVAL) {
                    // Finish received work even if retirement raced with receiving it.
                    Ok(input) => function(input),
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        Self { retire, handle }
    }

    fn shrink(workers: &mut Vec<Self>, count: usize) {
        Self::assert_not_current(&workers[count..]);

        let removed = workers.split_off(count);

        // Request every retirement before waiting for any running job to finish.
        for worker in &removed {
            worker.retire.store(true, Ordering::Relaxed);
        }

        Self::join_all(removed);
    }

    fn join_all(workers: Vec<Self>) {
        Self::assert_not_current(&workers);

        // Panic payloads can panic when dropped, so keep them until every worker is joined.
        let payloads: Vec<_> = workers
            .into_iter()
            .filter_map(|worker| worker.handle.join().err())
            .collect();

        for payload in payloads {
            drop_panic_payload(payload);
        }
    }

    fn assert_not_current(workers: &[Self]) {
        let current = thread::current().id();
        assert!(
            workers
                .iter()
                .all(|worker| worker.handle.thread().id() != current),
            "cannot join the current worker thread"
        );
    }
}

/// Runs queued one-shot jobs on a resizable group of threads.
///
/// Jobs use an unbounded queue, and submission does not wait for execution.
/// Use [`Self::with_scale`] to start workers immediately; [`Self::new`] starts
/// with none. Job execution order across workers is not guaranteed.
///
/// Dropping the pool detaches its workers. Call [`Self::join`] to wait for them
/// to finish. With no workers, finalizing the pool drops queued jobs without
/// executing them.
///
/// # Examples
///
/// ```
/// use fatij::ThreadPool;
///
/// let mut pool = ThreadPool::new();
/// pool.execute(|| println!("work started after scaling"));
/// pool.scale(2);
/// pool.join();
/// ```
#[derive(Debug)]
#[must_use = "dropping the pool detaches its workers; call join() to wait for completion"]
pub struct ThreadPool {
    sender: Sender<Box<dyn Job>>,
    receiver: Receiver<Box<dyn Job>>,
    workers: Vec<Worker>,
}

impl Default for ThreadPool {
    fn default() -> Self {
        Self::new()
    }
}

impl ThreadPool {
    /// Creates a pool with zero workers.
    ///
    /// Jobs can be queued immediately, but do not run until [`Self::scale`]
    /// starts workers. Use [`Self::with_scale`] to create an active pool.
    pub fn new() -> Self {
        let (sender, receiver) = mpmc::channel();
        Self {
            sender,
            receiver,
            workers: Vec::new(),
        }
    }

    /// Creates a pool and starts `workers` threads.
    ///
    /// A count of zero behaves like [`Self::new`].
    ///
    /// # Panics
    ///
    /// Panics if a worker thread cannot be spawned.
    pub fn with_scale(workers: usize) -> Self {
        let mut this = Self::new();
        this.scale(workers);
        this
    }

    /// Sets the worker count, waiting for excess workers to finish received jobs and stop.
    ///
    /// Shrinking can block indefinitely if a running job never returns. Once this returns,
    /// retired workers no longer consume jobs. Queued jobs remain available to the remaining
    /// workers, or to workers started by a later increase if the count is zero.
    ///
    /// # Panics
    ///
    /// Panics if a new worker thread cannot be spawned, or if shrinking would
    /// retire the calling worker. Self-retirement is rejected before changing
    /// the worker configuration.
    pub fn scale(&mut self, workers: usize) {
        if workers < self.workers.len() {
            Worker::shrink(&mut self.workers, workers);
            return;
        }

        for _ in 0..workers - self.workers.len() {
            let receiver = self.receiver.clone();
            let worker = Worker::spawn(receiver, |function| {
                // Catch job panics so the worker can continue taking jobs.
                if let Err(payload) = panic::catch_unwind(function) {
                    drop_panic_payload(payload);
                }
            });
            self.workers.push(worker);
        }
    }

    /// Queues a job for a worker without waiting for execution.
    ///
    /// The queue is unbounded. With zero workers, the job waits until a later
    /// [`Self::scale`] call starts workers. Job panics are caught when unwinding
    /// is enabled, allowing the worker to continue with other jobs.
    pub fn execute(&self, job: impl Job) {
        self.sender.send(Box::new(job)).unwrap();
    }

    /// Consumes the pool, closes its input, and waits for every worker to finish.
    ///
    /// Existing workers finish queued jobs. With zero workers, queued jobs are
    /// dropped without execution. A job that never returns can block joining
    /// indefinitely. Caught job panics are not propagated to the caller.
    ///
    /// # Panics
    ///
    /// Panics if called from one of this pool's own workers, before waiting for
    /// any worker. The consumed pool is dropped without joining its workers;
    /// initiate joining from another thread to complete shutdown.
    pub fn join(self) {
        Worker::assert_not_current(&self.workers);
        drop(self.receiver);
        drop(self.sender);
        Worker::join_all(self.workers);
    }
}

/// Applies a function to queued inputs on a resizable group of workers.
///
/// Both input and output queues are unbounded. The task owns one context in an
/// [`Arc`], and each worker borrows that same context for every invocation.
/// The function takes `&C` and consumes an input to produce an output. Outputs
/// are received in channel send order, which can differ from input order across
/// concurrent workers. [`Self::send`] returns a [`TaskId`]; every receive method
/// and iterator yields [`TaskOutput`], pairing that ID with `Ok(output)` or an
/// executor [`TaskError`]. Errors returned normally by the function stay inside `O`.
///
/// [`Self::new`] and [`Self::with_context`] start with zero workers. Use
/// [`Self::with_scale`] or [`Self::with_scale_and_context`] to start processing
/// immediately. Input and output values must be owned, sendable between threads,
/// and unwind-safe. The shared context must be [`Send`] and [`Sync`] so workers
/// can access it concurrently, and [`RefUnwindSafe`] so its reference can cross
/// a panic boundary. The context does not need to implement [`Clone`] or
/// [`UnwindSafe`]. Mutable shared state needs synchronization, such as a mutex
/// or atomics. Put state owned by an individual invocation in its input.
///
/// # Receiving and shutdown
///
/// A live task retains an output sender, so [`Self::recv`] and [`Self::iter`]
/// keep waiting when no output is available. Use [`Self::into_stream`] to close
/// input and receive a finite batch of remaining outputs. [`Self::join`]
/// discards outputs and waits for all workers.
///
/// Dropping the task detaches workers and drops buffered outputs directly.
/// Existing workers continue queued inputs and discard outputs they cannot
/// deliver, catching destructor panics in that worker-side disposal. With no
/// workers, [`Self::into_stream`] reports [`TaskError::Discarded`] for each queued
/// input and disposes of the inputs with panic protection. [`Self::join`] also
/// protects disposal, while discarding all completions. Implicit dropping uses
/// normal input drop behavior and does not report discarded inputs.
///
/// # Panics
///
/// With unwinding enabled, function panics produce [`TaskError::Panicked`] with
/// the input's ID; the worker continues processing. String panic messages are
/// copied into the error, and the original payload is disposed of with panic
/// protection. Changes already made to shared context are not rolled back;
/// a mutex held during a panic may become poisoned. A function or destructor
/// that never returns can still prevent a completion from arriving.
///
/// # Examples
///
/// ```
/// use fatij::Task;
///
/// let task = Task::with_scale(2, |number: u32| number * 2);
/// let first = task.send(3);
/// let second = task.send(5);
/// let stream = task.into_stream();
/// let mut outputs: Vec<_> = stream.iter().collect();
/// stream.join();
/// outputs.sort_unstable_by_key(|(id, _)| *id);
/// assert_eq!(outputs, vec![(first, Ok(6)), (second, Ok(10))]);
/// ```
#[derive(Debug)]
#[must_use = "receive outputs or call join(); dropping the task discards outputs and detaches workers"]
pub struct Task<I, O, C>
where
    I: 'static + Send + UnwindSafe,
    O: 'static + Send + UnwindSafe,
    C: 'static + Send + Sync + RefUnwindSafe,
{
    input: Channel<(TaskId, I)>,
    output: Channel<TaskOutput<O>>,
    next_id: AtomicUsize,
    workers: Vec<Worker>,
    context: Arc<C>,
    function: fn(&C, I) -> O,
}

impl<I, O> Task<I, O, fn(I) -> O>
where
    I: 'static + Send + UnwindSafe,
    O: 'static + Send + UnwindSafe,
{
    /// Creates a task with zero workers for a function without explicit context.
    ///
    /// `function` is a function pointer; noncapturing closures also coerce to
    /// this type. Use [`Self::with_context`] to supply captured data explicitly.
    /// Inputs wait until [`Self::scale`] starts workers.
    pub fn new(function: fn(I) -> O) -> Self {
        Self::with_context(function, |f, i| f(i))
    }

    /// Creates a task for `function` and starts `workers` threads.
    ///
    /// A count of zero behaves like [`Self::new`]. Noncapturing closures can be
    /// passed as `function`.
    ///
    /// # Panics
    ///
    /// Panics if a worker thread cannot be spawned.
    pub fn with_scale(workers: usize, function: fn(I) -> O) -> Self {
        Self::with_scale_and_context(workers, function, |f, i| f(i))
    }
}

impl<I, O, C> Task<I, O, C>
where
    I: 'static + Send + UnwindSafe,
    O: 'static + Send + UnwindSafe,
    C: 'static + Send + Sync + RefUnwindSafe,
{
    /// Creates a task with an explicit context and zero workers.
    ///
    /// Moves `context` into an [`Arc`] owned by the task. `function` borrows that
    /// same context as `&C` for each input. Workers hold their own `Arc` handles,
    /// keeping the context alive if the task is converted to a stream or dropped
    /// while they are running. The context itself is never cloned.
    pub fn with_context(context: C, function: fn(&C, I) -> O) -> Self {
        Self {
            input: Channel::new(),
            output: Channel::new(),
            next_id: AtomicUsize::new(0),
            workers: Vec::new(),
            context: Arc::new(context),
            function,
        }
    }

    /// Creates a task with an explicit context and starts `workers` threads.
    ///
    /// The context is moved into an [`Arc`] and borrowed by every invocation.
    /// Only the `Arc` handle is cloned for each worker; `C` need not implement
    /// [`Clone`]. A count of zero behaves like [`Self::with_context`]. Pass an
    /// `Arc` as the context if the caller also needs access to its data.
    ///
    /// # Panics
    ///
    /// Panics if a worker thread cannot be spawned.
    ///
    /// # Examples
    ///
    /// ```
    /// use fatij::Task;
    ///
    /// let task = Task::with_scale_and_context(1, 10, |offset, value: i32| offset + value);
    /// let id = task.send(2);
    /// let stream = task.into_stream();
    /// assert_eq!(stream.recv(), Some((id, Ok(12))));
    /// stream.join();
    /// ```
    pub fn with_scale_and_context(workers: usize, context: C, function: fn(&C, I) -> O) -> Self {
        let mut this = Self::with_context(context, function);
        this.scale(workers);
        this
    }

    /// Sets the worker count, waiting for excess workers to finish received inputs and stop.
    ///
    /// Shrinking can block indefinitely if a running function never returns. Once this returns,
    /// retired workers no longer consume inputs. Queued inputs remain available to the remaining
    /// workers, or to workers started by a later increase if the count is zero.
    ///
    /// # Panics
    ///
    /// Panics if a worker thread cannot be spawned, or if shrinking would
    /// retire the calling worker. Self-retirement is rejected before changing
    /// the worker configuration.
    pub fn scale(&mut self, workers: usize) {
        if workers < self.workers.len() {
            Worker::shrink(&mut self.workers, workers);
            return;
        }

        for _ in 0..workers - self.workers.len() {
            let receiver = self.input.receiver.clone();
            let sender = self.output.sender.clone();
            let context = Arc::clone(&self.context);
            let function = self.function;

            let worker = Worker::spawn(receiver, move |(id, input)| {
                let context = context.as_ref();
                // Borrow the shared context across the panic boundary; consume only the input.
                let result = panic::catch_unwind(move || function(context, input))
                    .map_err(TaskError::from_panic);
                if let Err(SendError(output)) = sender.send((id, result)) {
                    drop_catching_unwind(output);
                }
            });

            self.workers.push(worker);
        }
    }

    /// Queues an input and returns its task-local ID without waiting for processing.
    ///
    /// The queue is unbounded. With zero workers, inputs wait for a later
    /// [`Self::scale`] call. Every completion includes this ID and either the
    /// output or an executor error. Concurrent receivers can receive a completion
    /// before this call returns. IDs do not imply enqueue or completion order.
    ///
    /// # Panics
    ///
    /// Panics before enqueueing if the task's ID counter is exhausted. IDs are
    /// never reused within a task.
    pub fn send(&self, input: I) -> TaskId {
        let id = TaskId(
            self.next_id
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                    next.checked_add(1)
                })
                .expect("task ID space exhausted"),
        );
        self.input.sender.send((id, input)).unwrap();
        id
    }

    /// Blocks until a completion is available and removes it from the queue.
    ///
    /// The task holds an output sender for its lifetime, so this method has no
    /// end-of-stream signal. It can block indefinitely if no workers are running,
    /// all completions have been received, or a running function never returns. Use
    /// [`Self::recv_timeout`] to bound waiting, or [`Self::into_stream`] to
    /// receive until the task's remaining outputs are exhausted.
    pub fn recv(&self) -> TaskOutput<O> {
        self.output.receiver.recv().unwrap()
    }

    /// Waits up to `timeout` for a completion, returning `None` if the wait times out.
    ///
    /// A timeout does not cancel pending inputs. Already buffered completions are
    /// received without waiting.
    pub fn recv_timeout(&self, timeout: Duration) -> Option<TaskOutput<O>> {
        self.output.receiver.recv_timeout(timeout).ok()
    }

    /// Waits until `deadline` for a completion, returning `None` if the wait times out.
    ///
    /// Reaching the deadline does not cancel pending inputs. Already buffered
    /// completions are received without waiting.
    pub fn recv_deadline(&self, deadline: Instant) -> Option<TaskOutput<O>> {
        self.output.receiver.recv_deadline(deadline).ok()
    }

    /// Receives a completion immediately, or returns `None` if none is ready.
    ///
    /// This never waits for pending inputs to finish. `None` does not mean that
    /// all inputs have finished or that no further completions will arrive.
    pub fn try_recv(&self) -> Option<TaskOutput<O>> {
        self.output.receiver.try_recv().ok()
    }

    /// Returns an iterator whose next calls receive completions without waiting.
    ///
    /// Each call returns `None` if no completion is currently ready. Collecting this
    /// iterator therefore consumes only completions available before the first
    /// empty receive, without waiting for pending inputs to finish.
    pub fn try_iter(&self) -> impl Iterator<Item = TaskOutput<O>> {
        self.output.receiver.try_iter()
    }

    /// Returns an iterator that waits for each next completion.
    ///
    /// A live task retains an output sender, so this iterator keeps waiting
    /// after the currently available outputs are consumed. Limit iteration to
    /// a known count of submitted inputs, or use [`Self::into_stream`] before
    /// collecting a finite batch. Panicking inputs produce error completions.
    pub fn iter(&self) -> impl Iterator<Item = TaskOutput<O>> {
        self.output.receiver.iter()
    }

    /// Consumes the task, closes its input, and transfers its outputs and workers to a stream.
    ///
    /// Existing workers finish queued inputs. The stream reports end of input
    /// once all worker output senders are dropped and buffered outputs have
    /// been received. Call [`Stream::join`] after receiving to wait for thread
    /// cleanup as well.
    ///
    /// With zero workers, each queued input is disposed of with panic protection
    /// and produces a [`TaskError::Discarded`] completion with its original ID.
    /// Buffered completions are retained. Start workers with [`Self::scale`]
    /// first if queued inputs must be processed instead of discarded.
    /// Disposal runs on the calling thread; a destructor that never returns
    /// can prevent conversion from finishing.
    ///
    /// # Examples
    ///
    /// ```
    /// use fatij::Task;
    ///
    /// let task = Task::with_scale(1, |value: i32| value + 1);
    /// let id = task.send(41);
    /// let stream = task.into_stream();
    /// assert_eq!(stream.recv(), Some((id, Ok(42))));
    /// assert_eq!(stream.recv(), None);
    /// stream.join();
    /// ```
    pub fn into_stream(self) -> Stream<O> {
        let Channel {
            sender: input_sender,
            receiver: input_receiver,
        } = self.input;
        drop(input_sender);
        let Channel {
            sender: output_sender,
            receiver: stream,
        } = self.output;
        if self.workers.is_empty() {
            for (id, input) in input_receiver.iter() {
                drop_catching_unwind(input);
                output_sender.send((id, Err(TaskError::Discarded))).unwrap();
            }
        }
        drop(input_receiver);
        drop(output_sender);
        Stream {
            stream,
            workers: self.workers,
        }
    }

    /// Closes the input, discards remaining outputs, and waits for every worker to finish.
    ///
    /// Existing workers finish queued inputs while this method receives and
    /// discards completions. Panics from output destructors are caught individually.
    /// With zero workers, queued inputs are discarded without execution, with
    /// the same protection against destructor panics. Use [`Self::into_stream`]
    /// to receive their [`TaskError::Discarded`] completions instead.
    ///
    /// This can block indefinitely if a running function or an input or output
    /// destructor never returns. To keep completions, use [`Self::into_stream`] instead.
    ///
    /// # Panics
    ///
    /// Panics if called from one of this task's own workers, before closing
    /// channels or waiting for outputs or workers. The consumed task is dropped
    /// without joining; initiate joining from another thread to complete shutdown.
    pub fn join(self) {
        Worker::assert_not_current(&self.workers);
        let Channel {
            sender: input_sender,
            receiver: input_receiver,
        } = self.input;
        drop(input_sender);
        if self.workers.is_empty() {
            for (_, input) in input_receiver.iter() {
                drop_catching_unwind(input);
            }
        }
        drop(input_receiver);

        let Channel {
            sender,
            receiver: stream,
        } = self.output;
        drop(sender);

        let stream = Stream {
            stream,
            workers: self.workers,
        };
        stream.join();
    }
}

/// Receives remaining task outputs after input has been closed.
///
/// Created by [`Task::into_stream`]. Outputs can still arrive while existing
/// workers finish queued inputs. The stream ends once every worker drops its
/// output sender and all buffered outputs have been received. Outputs can
/// arrive in a different order from their corresponding inputs.
/// Every receive method and iterator yields [`TaskOutput`], preserving the
/// submitted [`TaskId`] for successful, panicked, and discarded inputs.
///
/// Call [`Self::join`] to discard remaining outputs and wait for thread cleanup.
/// Reaching the end of the stream alone does not join the threads.
///
/// Dropping the stream detaches workers and drops buffered outputs directly.
/// Existing workers continue queued inputs and discard undeliverable outputs
/// with protected disposal. Use [`Self::join`] to also protect disposal of
/// buffered outputs from destructor panics.
#[derive(Debug)]
#[must_use = "receive remaining outputs or call join(); dropping the stream discards outputs and detaches workers"]
pub struct Stream<O>
where
    O: 'static + Send + UnwindSafe,
{
    stream: Receiver<TaskOutput<O>>,
    workers: Vec<Worker>,
}

impl<O> Stream<O>
where
    O: 'static + Send + UnwindSafe,
{
    /// Blocks for the next completion, or returns `None` once the stream has ended.
    ///
    /// Buffered completions are received before end-of-stream is reported. This can
    /// wait indefinitely if a worker never finishes and no completion becomes ready.
    pub fn recv(&self) -> Option<TaskOutput<O>> {
        self.stream.recv().ok()
    }

    /// Waits up to `timeout` for a completion.
    ///
    /// Returns [`RecvTimeoutError::Timeout`] if the wait times out, or
    /// [`RecvTimeoutError::Disconnected`] once the stream has ended.
    /// Buffered completions are received before disconnection is reported.
    /// Timing out does not cancel worker execution.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<TaskOutput<O>, RecvTimeoutError> {
        self.stream.recv_timeout(timeout)
    }

    /// Waits until `deadline` for a completion.
    ///
    /// Returns [`RecvTimeoutError::Timeout`] if the deadline is reached, or
    /// [`RecvTimeoutError::Disconnected`] once the stream has ended.
    /// Buffered completions are received before disconnection is reported.
    /// Reaching the deadline does not cancel work.
    pub fn recv_deadline(&self, deadline: Instant) -> Result<TaskOutput<O>, RecvTimeoutError> {
        self.stream.recv_deadline(deadline)
    }

    /// Receives a completion immediately without waiting.
    ///
    /// Returns [`TryRecvError::Empty`] if no completion is currently ready, or
    /// [`TryRecvError::Disconnected`] once the stream has ended.
    /// Buffered completions are received before disconnection is reported.
    pub fn try_recv(&self) -> Result<TaskOutput<O>, TryRecvError> {
        self.stream.try_recv()
    }

    /// Returns an iterator whose next calls receive completions without waiting.
    ///
    /// A call returns `None` when no completion is ready, including after the stream
    /// ends. Collecting it can stop before workers produce their remaining
    /// completions; use [`Self::iter`] to wait for the whole batch.
    pub fn try_iter(&self) -> impl Iterator<Item = TaskOutput<O>> {
        self.stream.try_iter()
    }

    /// Returns a blocking iterator over the remaining completions.
    ///
    /// Each next call waits for a completion or the end of the stream. Iteration
    /// ends after every output sender has been dropped and the queue is empty.
    /// Call [`Self::join`] afterward to wait for thread cleanup.
    pub fn iter(&self) -> impl Iterator<Item = TaskOutput<O>> {
        self.stream.iter()
    }

    /// Discards remaining outputs and waits for every worker to finish.
    ///
    /// Receives until the output channel disconnects, discarding each output
    /// with protection against destructor panics, then joins every worker.
    /// This can block indefinitely if a worker or an output destructor never
    /// finishes. Outputs already handed to the caller are not disposed of here.
    ///
    /// # Panics
    ///
    /// Panics if called from one of this stream's own workers, before draining
    /// outputs or waiting for workers. The consumed stream is dropped without
    /// joining; initiate joining from another thread to complete shutdown.
    pub fn join(self) {
        Worker::assert_not_current(&self.workers);

        for output in self.stream.iter() {
            drop_catching_unwind(output);
        }

        Worker::join_all(self.workers);
    }
}

#[cfg(test)]
mod tests;
