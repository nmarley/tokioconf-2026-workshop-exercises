// Exercise 2.1: block_on
//
// Build a minimal executor that can run one future to completion. This is
// similar to `tokio::runtime::Runtime::block_on()`. It takes a future, polls
// it in a loop until it completes, and returns.
//
// ## Background
//
// In exercises 1.x, the provided executor (ex1_executor.rs) ran your futures.
// Now you build that executor yourself.
//
// An executor's job:
//   1. Wrap the future in a Task (heap-allocated, reference-counted with Arc).
//   2. Poll the task. If Ready, we're done.
//   3. If Pending, park the thread (thread::park()). The waker will unpark it.
//   4. Go to 2.
//
// The key abstraction is the ArcWake trait (from the `futures` crate). When
// you implement ArcWake for Task, the runtime can convert an Arc<Task> into a
// std::task::Waker. Calling wake() on that waker runs your ArcWake::wake_by_ref
// implementation, which should re-queue the task and unpark the executor thread.
//
// We heap-allocate the future inside the Task (Box::pin) rather than pinning
// it on the stack. This sets up exercise 2.2 where we'll spawn multiple tasks
// the same way.
//
// ## What to build
//
//   - Executor struct: holds a run queue (VecDeque<Arc<Task>> behind a Mutex)
//     and a Thread handle (for unpark)
//   - Task struct: holds the future (Mutex<Pin<Box<dyn Future<...>>>>) and an
//     Arc<Executor> back-reference
//   - ArcWake for Task: push the task onto the queue, then unpark the thread
//   - block_on() function: create the executor, wrap the future, run the poll loop
//
// See hints/ex2_1/ if you get stuck.

use futures::task::ArcWake;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::thread::{self, Thread};

// TODO: Define an Executor struct.
//
// Fields:
//   - queue: Mutex<VecDeque<Arc<Task>>>   (the run queue, shared with tasks)
//   - thread: Thread                       (so wakers can call thread.unpark())

// TODO: Define a Task struct.
//
// Fields:
//   - future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>
//       The Mutex is required because ArcWake needs Task: Sync. On a
//       single-threaded executor, the lock is never actually contended.
//       Pin<Box<...>> heap-allocates the future and pins it so poll() is safe.
//   - executor: Arc<Executor>
//       Back-reference so the waker can find the run queue and thread handle.

// TODO: Implement ArcWake for Task.
//
// The `futures` crate provides the ArcWake trait. Implementing it lets you
// convert an Arc<Task> into a std::task::Waker using futures::task::waker().
//
// wake_by_ref(&Arc<Task>) should:
//   1. Push a clone of the Arc<Task> onto the executor's run queue
//   2. Call executor.thread.unpark() to wake the executor

/// Run a future to completion on a new single-threaded executor.
///
/// This function blocks the current thread until the future completes.
pub fn block_on(fut: impl Future<Output = ()> + Send + 'static) {
    // TODO: Implement the executor loop.
    //
    // Steps:
    //   1. Create an Executor (with an empty queue and thread::current())
    //   2. Wrap `fut` in a Task and push it onto the queue
    //   3. Loop:
    //      a. Pop tasks from the queue and poll each one
    //         - Create a waker from the Arc<Task> using futures::task::waker()
    //         - Create a Context from the waker
    //         - Lock the task's future and poll it
    //         - If Ready: we're done, return
    //         - If Pending: the waker will re-enqueue the task later
    //      b. When the queue is empty, park the thread (thread::park())
    //
    // Watch out: don't hold the queue lock while polling. If the future
    // calls wake() (which locks the queue), you'll deadlock.
    let _ = fut;
    todo!("implement block_on")

    // crate::solutions::ex2_1::block_on(fut)
}

pub fn run() {
    use std::time::Duration;

    println!("=== Exercise 2.1: block_on ===\n");

    // A future that completes immediately
    print!("Ready immediately... ");
    block_on(async {});
    println!("ok");

    // YieldNow from exercise 1.1
    print!("YieldNow... ");
    block_on(async {
        crate::solutions::ex1_1::yield_now().await;
    });
    println!("ok");

    // Delay from exercise 1.2
    print!("Delay (200ms)... ");
    block_on(async {
        crate::solutions::ex1_2::Delay::new(Duration::from_millis(200)).await;
    });
    println!("ok");

    // Oneshot from exercise 1.3
    print!("Oneshot... ");
    block_on(async {
        let (tx, rx) = crate::solutions::ex1_3::oneshot();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            tx.send("hello from the other side");
        });
        let val = rx.await;
        println!("received: {val}");
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ready_immediately() {
        block_on(async {});
    }

    #[test]
    fn yield_once() {
        block_on(async {
            crate::solutions::ex1_1::yield_now().await;
        });
    }

    #[test]
    fn wake_from_another_thread() {
        use std::time::Duration;

        block_on(async {
            crate::solutions::ex1_2::Delay::new(Duration::from_millis(50)).await;
        });
    }

    #[test]
    fn oneshot_from_thread() {
        block_on(async {
            let (tx, rx) = crate::solutions::ex1_3::oneshot();
            std::thread::spawn(move || {
                tx.send(42);
            });
            let val = rx.await;
            assert_eq!(val, 42);
        });
    }

    #[test]
    fn multiple_yields() {
        block_on(async {
            for _ in 0..5 {
                crate::solutions::ex1_1::yield_now().await;
            }
        });
    }
}
