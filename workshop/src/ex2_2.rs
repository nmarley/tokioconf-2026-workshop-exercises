// Exercise 2.2: spawn
//
// Extend the exercise 2.1 executor with the ability to spawn additional tasks.
// This is how `tokio::spawn()` works. From inside any async code, you can kick
// off a new independent task that runs concurrently.
//
// ## Background
//
// In exercise 2.1, block_on() ran a single future. When any task returned
// Ready, the executor was done. With spawn(), multiple tasks run concurrently
// on the same thread, and the executor must keep running until the *main*
// future completes (spawned tasks may still be in flight).
//
// spawn() is a free function, not a method on the executor. How does it find
// the run queue? Through a thread-local variable. block_on() stores the
// Arc<Executor> in a thread-local before entering the poll loop, and spawn()
// reads it to push new tasks.
//
// ## What changes from exercise 2.1
//
//   - Define a thread-local `Cell<Option<Arc<Executor>>>`. block_on() sets it
//     before the loop and clears it when done.
//   - spawn() reads the thread-local, wraps the future in a Task, pushes it
//     onto the queue, and unparks the executor thread.
//   - block_on() must track whether the main future has completed (use an
//     AtomicBool flag, wrap the main future in an async block that sets it).
//   - Keep running until the main future completes, not just until any task
//     returns Ready.
//   - If the queue is empty but the main future hasn't completed, that means
//     a task returned Pending without arranging a wake. Panic with the message
//     "no runnable tasks" (the test expects this exact substring).
//
// The Executor, Task, and ArcWake implementation are the same as exercise 2.1.
// They are provided below as a starting point.

use futures::task::ArcWake;
use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::thread::{self, Thread};

// --- Provided: Executor and Task (same as exercise 2.1) ---

struct Executor {
    queue: Mutex<VecDeque<Arc<Task>>>,
    thread: Thread,
}

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    executor: Arc<Executor>,
}

// TODO: Implement ArcWake for Task (same as exercise 2.1).
// wake_by_ref should push the task onto the queue and unpark the thread.

// TODO: Define a thread-local to store the current executor.
//
// Use: thread_local! { static CURRENT: Cell<Option<Arc<Executor>>> = const { Cell::new(None) }; }
//
// Cell works here because we only set/take the value, never borrow it.
// block_on() sets this before entering the poll loop. spawn() reads it
// to find the run queue.

/// Spawn a new task onto the current executor.
///
/// Panics if called outside of block_on().
pub fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    // TODO: Implement spawn.
    //
    // 1. Read the Arc<Executor> from the thread-local (take it out)
    //    - Panic if None (spawn called outside block_on)
    // 2. Create a Task wrapping the future
    // 3. Push the task onto the executor's queue
    // 4. Unpark the executor thread
    // 5. Put the Arc<Executor> back into the thread-local
    //
    // The take-then-put-back pattern is needed because Cell::take() moves
    // the value out. You must put it back or the next spawn() will panic.
    let _ = fut;
    todo!("implement spawn")
}

/// Run a future to completion, allowing it to spawn additional tasks.
pub fn block_on(fut: impl Future<Output = ()> + Send + 'static) {
    // TODO: Implement block_on with spawn support.
    //
    // 1. Create the Executor (same as 2.1)
    // 2. Store it in the thread-local so spawn() works
    // 3. Create an AtomicBool `done` flag. Wrap the main future in an async
    //    block that awaits it and then sets `done` to true. This is how we
    //    distinguish the main future completing from spawned tasks completing.
    // 4. Wrap that async block in a Task, push onto the queue
    // 5. Poll loop (same as 2.1), but:
    //    - After draining the queue, check if `done` is true. If so, clean
    //      up the thread-local and return.
    //    - If the queue is empty AND done is false, a task is stuck. Panic
    //      with a message containing "no runnable tasks".
    //    - Otherwise, park the thread and wait for a wake.
    // 6. Clear the thread-local when done
    let _ = fut;
    todo!("implement block_on with spawn support")
}

pub fn run() {
    println!("=== Level 2: spawn ===\n");

    block_on(async {
        println!("Main task starting");

        spawn(async {
            println!("  Spawned task 1 running");
        });

        spawn(async {
            println!("  Spawned task 2 running");
        });

        // Yield to let spawned tasks run
        crate::solutions::ex1_1::yield_now().await;

        println!("Main task finishing");
    });

    println!("All tasks completed.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    async fn yield_now() {
        crate::solutions::ex1_1::yield_now().await;
    }

    #[test]
    fn spawn_runs_task() {
        let ran = Arc::new(AtomicBool::new(false));
        let ran2 = ran.clone();

        block_on(async move {
            spawn(async move {
                ran2.store(true, Ordering::SeqCst);
            });
            yield_now().await;
        });

        assert!(ran.load(Ordering::SeqCst), "spawned task should have run");
    }

    #[test]
    fn spawn_multiple_tasks() {
        let count = Arc::new(AtomicUsize::new(0));
        let c = count.clone();

        block_on(async move {
            for _ in 0..5 {
                let c = c.clone();
                spawn(async move {
                    c.fetch_add(1, Ordering::SeqCst);
                });
            }
            yield_now().await;
        });

        assert_eq!(
            count.load(Ordering::SeqCst),
            5,
            "all 5 spawned tasks should have run"
        );
    }

    #[test]
    fn spawned_task_can_spawn() {
        let ran = Arc::new(AtomicBool::new(false));
        let ran2 = ran.clone();

        block_on(async move {
            spawn(async move {
                spawn(async move {
                    ran2.store(true, Ordering::SeqCst);
                });
                yield_now().await;
            });
            yield_now().await;
            yield_now().await;
        });

        assert!(
            ran.load(Ordering::SeqCst),
            "nested spawned task should have run"
        );
    }

    #[test]
    fn main_future_completes_last() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let o = order.clone();

        block_on(async move {
            let o2 = o.clone();
            spawn(async move {
                o2.lock().unwrap().push("spawned");
            });
            yield_now().await;
            o.lock().unwrap().push("main");
        });

        let order = order.lock().unwrap();
        assert_eq!(*order, vec!["spawned", "main"]);
    }

    #[test]
    #[should_panic(expected = "no runnable tasks")]
    fn panic_on_stuck_executor() {
        block_on(async {
            std::future::poll_fn(|_cx| Poll::<()>::Pending).await;
        });
    }

    #[test]
    fn spawn_with_delay() {
        block_on(async {
            spawn(async {
                crate::solutions::ex1_2::Delay::new(Duration::from_millis(50)).await;
            });

            crate::solutions::ex1_2::Delay::new(Duration::from_millis(100)).await;
        });
    }

    #[test]
    fn spawn_with_oneshot() {
        block_on(async {
            let (tx, rx) = crate::solutions::ex1_3::oneshot();

            spawn(async move {
                tx.send(42);
            });

            yield_now().await;
            let val = rx.await;
            assert_eq!(val, 42);
        });
    }
}
