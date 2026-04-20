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
//   - Return as soon as the main future completes, even if spawned tasks are
//     still pending. (Real Tokio does the same: spawned tasks that haven't
//     completed by the time block_on returns are simply dropped.)
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

impl Executor {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            thread: thread::current(),
        }
    }
}

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    executor: Arc<Executor>,
}

impl ArcWake for Task {
    // should push the task onto the queue and unpark the thread.
    fn wake_by_ref(t: &Arc<Task>) {
        t.executor.queue.lock().unwrap().push_back(t.clone());
        t.executor.thread.unpark();
    }
}

// TODO: Define a thread-local to store the current executor.
//
// Use: thread_local! { static CURRENT: Cell<Option<Arc<Executor>>> = const { Cell::new(None) }; }
//
// Cell works here because we only set/take the value, never borrow it.
// block_on() sets this before entering the poll loop. spawn() reads it
// to find the run queue.

thread_local! {
    static CURRENT: Cell<Option<Arc<Executor>>> = const { Cell::new(None) };
}

/// Spawn a new task onto the current executor.
///
/// Panics if called outside of block_on().
pub fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    // 1. Read the Arc<Executor> from the thread-local (take it out)
    //    - Panic if None (spawn called outside block_on)
    // 2. Create a Task wrapping the future
    // 3. Push the task onto the executor's queue
    // 4. Unpark the executor thread
    // 5. Put the Arc<Executor> back into the thread-local
    //
    // The take-then-put-back pattern is needed because Cell::take() moves
    // the value out. You must put it back or the next spawn() will panic.

    let executor = CURRENT
        .with(|cell| cell.take())
        .expect("spawn called outside block_on");
    let task = Arc::new(Task {
        future: Mutex::new(Box::pin(fut)),
        executor: executor.clone(),
    });
    executor.queue.lock().unwrap().push_back(task);
    executor.thread.unpark();
    CURRENT.with(|cell| cell.set(Some(executor)));
}

/// Run a future to completion, allowing it to spawn additional tasks.
pub fn block_on(fut: impl Future<Output = ()> + Send + 'static) {
    // 1. Create the Executor (same as 2.1)
    // 2. Store it in the thread-local so spawn() works
    // 3. Create an AtomicBool `done` flag. Wrap the main future in an async
    //    block that awaits it and then sets `done` to true. This is how we
    //    distinguish the main future completing from spawned tasks completing.
    // 4. Wrap that async block in a Task, push onto the queue
    // 5. Poll loop (same as 2.1), but:
    //    - Drain the queue (pop and poll until empty).
    //    - After draining, if `done` is true, clean up the thread-local and
    //      return (even if spawned tasks are still pending).
    //    - Otherwise, park the thread and wait for a wake.
    // 6. Clear the thread-local before returning.
    let executor = Arc::new(Executor::new());
    CURRENT.with(|cell| cell.set(Some(executor.clone())));

    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();

    let wrapped = async move {
        fut.await;
        done_clone.store(true, Ordering::SeqCst);
    };

    let task = Arc::new(Task {
        future: Mutex::new(Box::pin(wrapped)),
        executor: executor.clone(),
    });
    executor.queue.lock().unwrap().push_back(task);

    loop {
        loop {
            let task = executor.queue.lock().unwrap().pop_front();
            let Some(task) = task else { break };
            let waker = futures::task::waker(task.clone());
            let cx = &mut Context::from_waker(&waker);
            let _ = task.future.lock().unwrap().as_mut().poll(cx);
        }
        if done.load(Ordering::SeqCst) {
            CURRENT.with(|cell| cell.take());
            return;
        }
        thread::park();
    }
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
    fn returns_when_main_completes_even_with_pending_task() {
        // block_on() must return as soon as the main future completes, even if
        // a spawned task is still pending. A buggy implementation that waits
        // for *all* tasks would block on the 2-second Delay below.
        let start = std::time::Instant::now();
        block_on(async {
            spawn(async {
                crate::solutions::ex1_2::Delay::new(Duration::from_secs(2)).await;
            });
            // Yield once so the spawned task gets polled and registers its waker.
            yield_now().await;
        });
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(500),
            "block_on should return when main completes, not wait for spawned \
             tasks (took {elapsed:?})"
        );
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
