// Exercise 1.1: YieldNow
//
// Implement a future that yields control back to the executor once, then
// completes. This is what tokio::task::yield_now() does.
//
// Behavior:
//   - First poll  → return Pending (but wake immediately so the executor polls again)
//   - Second poll → return Ready(())
//
// ## Background
//
// A Future is a state machine. The executor calls poll() repeatedly. Each call
// advances the state machine one step. The future returns Poll::Pending when it
// is not yet complete and Poll::Ready(value) when it is.
//
// The `cx` argument carries a Waker. Calling `cx.waker().wake_by_ref()` tells
// the executor "poll me again." Returning Pending without arranging a wake() is
// a bug: the executor parks and nobody wakes it. This executor enforces that
// with a 5-second timeout.
//
// ## What to do
//
// 1. Add a field to YieldNow that tracks whether poll() has been called before.
// 2. In yield_now(), construct YieldNow in its initial state.
// 3. In poll():
//    - If this is the first poll, record that we've yielded, wake the executor,
//      and return Pending.
//    - If this is the second poll, return Ready(()).
//
// Hint: `self.field` works on Pin<&mut Self> when the field type is Unpin
// (all primitive types, including bool, are Unpin).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

pub struct YieldNow {
    // TODO: add a field to track whether we have already yielded
}

pub fn yield_now() -> YieldNow {
    todo!()
}

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        // TODO: check if we already yielded.
        //   - If not: mark that we have, wake the executor, return Pending.
        //   - If so: return Ready(()).
        todo!()
    }
}

pub fn run() {
    let fut = yield_now();
    // let fut = crate::solutions::ex1_1::yield_now();

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
    fn test_yield_now() {
        let ((), stats) = ex1_executor::run(yield_now());
        assert_eq!(
            stats.poll_count, 2,
            "YieldNow should complete in exactly 2 polls"
        );
    }
}
