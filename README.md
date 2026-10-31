# fatij

A small, dependency-free Rust crate for running work on resizable groups of
threads.

- `ThreadPool` runs independent closures that each execute once.
- `Task` applies a function to queued inputs and collects identified success or error results.
- `Stream` receives the remaining results after a task stops accepting inputs.

The name **fatij** comes from Abruzzese dialect and means “to work” or “jobs.”

## Requirements

**Nightly Rust is required** because the crate uses the unstable
`std::sync::mpmc` channel API. The manifest's `rust-version = "1.95"` does not
mean the crate builds on stable Rust.

The repository's `rust-toolchain.toml` selects the tested `nightly-2026-10-31`
toolchain automatically when running Cargo commands from this checkout and
requests Clippy and rustfmt. CI checks both this pinned toolchain and current
nightly.

Add the crate from crates.io:

```toml
[dependencies]
fatij = "0.1"
```

Build that project with `cargo +nightly build`. Callers do not need to enable
`mpmc_channel` in their own crate. A dependency's toolchain file does not select
the toolchain for the consuming project.

To use the same toolchain as this checkout, build with
`cargo +nightly-2026-10-31 build` instead.

## Run one-shot jobs

Start workers with `ThreadPool::with_scale`, submit owned closures, and call
`join` to wait for completion:

```rust
use fatij::ThreadPool;
use std::sync::mpsc;

let pool = ThreadPool::with_scale(2);
let (sender, receiver) = mpsc::channel();

pool.execute(move || {
    sender.send(String::from("finished")).unwrap();
});

pool.join();
assert_eq!(receiver.recv().unwrap(), "finished");
```

Jobs implement `FnOnce() + Send + UnwindSafe + 'static`. They can own captured
values; any borrowed data must have a `'static` lifetime.
Use a channel or shared state when a job needs to return a result.

## Process a batch of inputs

`Task` takes a function pointer; a closure that captures no values can also be
used. `send(input)` returns a `TaskId`, and each completion is a
`(TaskId, Result<O, TaskError>)` tuple, also named `TaskOutput<O>`. Completions
arrive in channel send order, which can differ from input order when multiple
workers run concurrently.

```rust
use fatij::Task;

let task = Task::with_scale(2, |number: u32| number * number);
for number in 1..=4 {
    task.send(number);
}

// Close the input and keep receiving the results of queued work.
let stream = task.into_stream();
let mut squares: Vec<_> = stream.iter().map(|(_, result)| result.unwrap()).collect();
stream.join();

squares.sort_unstable();
assert_eq!(squares, vec![1, 4, 9, 16]);
```

Use `Task::recv` for a blocking receive while keeping the input open, or
`recv_timeout`, `recv_deadline`, and `try_recv` to limit waiting. A live `Task`
retains an output sender: `Task::iter()` keeps waiting after its current outputs
are consumed. Convert a completed batch with `into_stream()` before collecting
all remaining outputs. A stream ends after every worker drops its output sender
and all buffered outputs have been received.

Every receive method and iterator preserves the submitted ID, including errors:

| API | Return type |
| --- | --- |
| `Task::send(input)` | `TaskId` |
| `Task::recv()` | `TaskOutput<O>` |
| `Stream::recv()` | `Option<TaskOutput<O>>` |
| `Task::recv_timeout`, `Task::recv_deadline`, `Task::try_recv` | `Option<TaskOutput<O>>` |
| `Stream::recv_timeout`, `Stream::recv_deadline` | `Result<TaskOutput<O>, RecvTimeoutError>` |
| `Stream::try_recv` | `Result<TaskOutput<O>, TryRecvError>` |
| `iter`, `try_iter` on either type | Iterator of `TaskOutput<O>` |

Stream receive errors distinguish waiting for more work from reaching the end
of the stream. The standard `std::sync::mpsc::RecvTimeoutError` has `Timeout`
and `Disconnected` variants; `std::sync::mpsc::TryRecvError` has `Empty` and
`Disconnected` variants. Buffered completions are returned before
`Disconnected`. These receive errors are separate from the `TaskError` inside
each completion, which describes an individual input's execution.

For example, poll for results while allowing periodic progress or cancellation
checks between waits:

```rust
use fatij::Task;
use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

let task = Task::with_scale(1, |number: i32| number * 2);
let id = task.send(21);
let stream = task.into_stream();
let mut outputs = Vec::new();

loop {
    match stream.recv_timeout(Duration::from_millis(10)) {
        Ok(completion) => outputs.push(completion),
        Err(RecvTimeoutError::Timeout) => {
            // Check cancellation or report progress before waiting again.
        }
        Err(RecvTimeoutError::Disconnected) => break,
    }
}

assert_eq!(outputs, vec![(id, Ok(42))]);
stream.join();
```

IDs are unique within one task and remain valid across worker resizes. They
are opaque counter values, not UUIDs; IDs from separate tasks can compare equal.
Concurrent submission does not guarantee that ID order matches enqueue,
execution, or completion order. A concurrent receiver can receive a completion
before `send` returns. If a task exhausts its ID counter, `send` panics before
enqueueing the input instead of reusing an ID.

### Pass context to a task

Use `with_scale_and_context` for functions that need configuration or shared
data. The task moves the context into an internal `Arc<C>` and each invocation
borrows the same value through `&C`. Only `Arc` handles are cloned when workers
start; the context itself is never cloned. Contexts must implement
`Send + Sync + RefUnwindSafe + 'static`. Neither `Clone` nor `UnwindSafe` is
required.

```rust
use fatij::Task;

let prefix = String::from("hello");
let task = Task::with_scale_and_context(2, prefix, |prefix, name: String| {
    format!("{prefix}, {name}")
});
let id = task.send(String::from("Ada"));

let stream = task.into_stream();
assert_eq!(stream.recv(), Some((id, Ok(String::from("hello, Ada")))));
assert_eq!(stream.recv(), None);
stream.join();
```

All workers access the context concurrently, so mutable shared state needs
synchronization, such as a `Mutex` or atomics. Non-`Clone` contexts work directly:

```rust
use fatij::Task;
use std::sync::Mutex;

let task = Task::with_scale_and_context(2, Mutex::new(0), |total, value: i32| {
    let mut total = total.lock().unwrap();
    *total += value;
    *total
});
task.send(1);
task.send(2);

let stream = task.into_stream();
assert_eq!(stream.iter().map(|(_, result)| result.unwrap()).max(), Some(3));
stream.join();
```

Each worker holds an `Arc` handle that keeps the context alive while it runs,
even if the task is converted to a stream or dropped. Pass an `Arc` as the
context if the caller also needs access to the data after moving it into the task.

Context functions use `fn(&C, I) -> O`. Context mutations are shared across
invocations; put state owned by an individual invocation in its input `I`,
or explicitly create an owned copy inside the function when needed.

## Resize workers and shut down

`scale(n)` sets the requested worker count for either a pool or a task.
Increasing it starts workers. Decreasing it requests retirement, lets excess
workers finish work they have received, and joins them before returning.
Shrinking can wait indefinitely if that work never returns.

Call `join()` from a thread outside the worker group being joined. Calling it
from one of that group's own workers panics before waiting, since a worker
cannot wait for its own completion. The call consumes the pool, task, or stream;
rejection drops it with normal drop behavior and does not complete shutdown.
A job can instead send a shutdown request or transfer the owner to another
thread that calls `join()`.

Pools, tasks, and streams are marked `#[must_use]` so ignoring a returned owner
produces a compiler warning. Use `drop(owner)` explicitly when detaching is intended.

Shrinking also panics if the workers selected for retirement include the caller.
This rejection leaves the worker configuration unchanged. A worker can still
grow its group, request the same count, or shrink it while retaining itself.

`new()` and `with_context()` start with **zero workers**. Submitted work waits
until workers are started. Scaling to zero pauses execution after the resize
returns; queued work can be resumed by increasing the count:

```rust
use fatij::Task;

let mut task = Task::new(|number: i32| number + 1);
let id = task.send(41);
assert_eq!(task.try_recv(), None);

task.scale(1);
let stream = task.into_stream();
assert_eq!(stream.recv(), Some((id, Ok(42))));
stream.join();
```

| Operation | Behavior |
| --- | --- |
| `ThreadPool::join()` | Close the input and wait for existing workers to finish queued jobs. |
| `Task::into_stream()` | Close the input and transfer remaining completions and worker handles to a `Stream`; report queued inputs as discarded if there are no workers. |
| `Task::join()` | Close the input, discard outputs with protected disposal, and join every worker. |
| `Stream::join()` | Discard remaining outputs with protected disposal and join every worker. |
| Drop a pool, task, or stream | Detach workers without waiting; existing workers can continue queued work. |

If there are no workers when a pool or task is finalized, its queued inputs are
discarded without execution. For a task, `into_stream()` produces
`(id, Err(TaskError::Discarded))` for each queued input and keeps any buffered
completions. `join()` discards inputs and completions. Both methods dispose of
queued task inputs individually with protection against destructor panics.
Disposal runs on the calling thread and can block if an input destructor never
returns. Start workers before finalizing if queued inputs must run.

```rust
use fatij::{Task, TaskError};

let task = Task::new(|number: i32| number * 2); // Zero workers.
let id = task.send(21);
let stream = task.into_stream();
assert_eq!(stream.recv(), Some((id, Err(TaskError::Discarded))));
assert_eq!(stream.recv(), None);
stream.join();
```

Scaling to zero only pauses processing; it does not discard inputs. Implicit
drop retains normal input drop behavior and does not report discarded inputs.

Dropping a task or stream disconnects the output receiver. Workers discard
outputs they can no longer deliver, catching destructor panics during that
disposal. Buffered outputs dropped with the receiver are dropped directly;
their destructor panics are not caught by implicit dropping. Use explicit
`join()` for protected disposal of remaining outputs and to wait for workers.

Input and output queues are unbounded. Submission does not wait for execution,
and unread outputs can accumulate in memory.

## Errors and panics

When unwinding is enabled, workers catch job and task-function panics. A
panicking task input produces `(id, Err(TaskError::Panicked(message)))`, and
the worker continues with later inputs. The message is `Some(String)` for
`String` and `&str` panic payloads, and `None` for other payload types. Raw panic
payloads are disposed of internally instead of transferring their destructors
to the receiver. `TaskError` implements `Display` and `std::error::Error`.

```rust
use fatij::{Task, TaskError};

let task = Task::with_scale(1, |number: i32| {
    assert_ne!(number, 0, "zero is not allowed");
    number * 2
});
let failed = task.send(0);
let succeeded = task.send(21);
let stream = task.into_stream();
assert!(matches!(stream.recv(),
    Some((id, Err(TaskError::Panicked(_)))) if id == failed));
assert_eq!(stream.recv(), Some((succeeded, Ok(42))));
assert_eq!(stream.recv(), None);
stream.join();
```

Ordinary function errors remain part of `O`: if the function returns
`Result<T, E>`, an application error arrives as `(id, Ok(Err(error)))`.
`TaskError::Panicked` and `TaskError::Discarded` describe executor failures.

After `into_stream()` returns successfully, receives across the task and its
stream provide one completion per submitted input, provided execution and
cleanup finish without process abort. A function or destructor that never
returns can still prevent completion. Dropping the consumer forfeits remaining
completions; a live task's blocking receives still have no end-of-stream signal.

Changes already made to shared context are not rolled back after a panic.
A mutex held by the panicking function may become poisoned; later invocations
must handle that according to the application's recovery policy.

The standard panic hook still runs. Builds using `panic = "abort"`, and panics
that abort the process during unwinding, cannot recover through these catches.

Caught panic payloads are normally dropped. If a payload's destructor panics,
its replacement panic payload and anything it owns are deliberately leaked to
avoid another cleanup panic. The same policy is used for panics while discarding
outputs during explicit joining or after failed sends, and queued task inputs
during zero-worker finalization.

Failure to spawn a thread propagates from the constructor or `scale()`.
Arbitrary destructors outside the protected job and output-disposal paths can
also panic on the thread that drops them.

## Development

Run these commands from the crate directory:

```sh
cargo fmt --check
cargo test --locked
cargo test --locked --release
cargo clippy --locked --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo package --locked
```

GitHub Actions runs these checks on pushes, pull requests, and a weekly schedule
using both `nightly-2026-10-31` and current nightly. Packaging requires a clean
working tree; use `cargo package --locked --allow-dirty` to check local changes.

The README is also the crate-level rustdoc, so `cargo test` compiles and
runs its Rust examples together with the API documentation examples. Generated
API documentation is available at `target/doc/fatij/index.html`.

## License

Licensed under the MIT license. See
[LICENSE](https://github.com/daddinuz/fatij/blob/HEAD/LICENSE).
