// Exercise 1.2: Delay
//
// Implement a future that completes after a duration has elapsed. This is a
// simplified version of tokio::time::sleep().
//
// ## Background
//
// Unlike YieldNow, the completion event is external. Time must actually pass.
// The future itself does not sleep. Instead, a background thread sleeps for
// the requested duration and then signals the future.
//
// This introduces a pattern you will see over and over in async Rust:
//
//   1. Shared state between the future and an external event source.
//   2. The external source sets a "complete" flag and calls wake().
//   3. The future's poll() checks the flag. If set, return Ready. If not,
//      store the waker so the external source can call it later.
//
// The shared state must be behind Arc<Mutex<...>> because the future and the
// background thread access it from different threads.
//
// ## What to do
//
// 1. Define a shared state struct with two fields:
//    - A bool tracking whether the delay has elapsed
//    - An Option<Waker> so the background thread can wake the executor
//
// 2. In Delay::new():
//    - Create the shared state (wrapped in Arc<Mutex<...>>)
//    - Spawn a thread that sleeps for `dur`, then locks the state, sets the
//      bool, and calls wake() on the stored waker (if any)
//    - Return a Delay holding a clone of the Arc
//
// 3. In poll():
//    - Lock the shared state
//    - If the delay has elapsed, return Ready(())
//    - Otherwise, store cx.waker().clone() in the shared state and return
//      Pending. The background thread will call wake() when the time is up.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

pub struct Delay {
    // TODO: add a field holding shared state (Arc<Mutex<...>>)
}

impl Delay {
    pub fn new(dur: Duration) -> Self {
        // TODO: create the shared state, spawn a timer thread, return Delay
        Delay {}
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // TODO: lock the shared state, check if done, store waker if not
        todo!()
    }
}

pub fn run() {
    let fut = Delay::new(Duration::from_millis(200));
    // let fut = crate::solutions::ex1_2::Delay::new(Duration::from_millis(200));

    let ((), stats) = crate::ex1_executor::run(fut);
    println!(
        "  Polls: {}, Time: {:.0?}",
        stats.poll_count, stats.total_time
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ex1_executor;

    #[test]
    fn delay() {
        let ((), stats) = ex1_executor::run(Delay::new(Duration::from_millis(100)));
        assert_eq!(
            stats.poll_count, 2,
            "Delay should complete in exactly 2 polls"
        );
        assert!(stats.total_time >= Duration::from_millis(100));
    }
}
