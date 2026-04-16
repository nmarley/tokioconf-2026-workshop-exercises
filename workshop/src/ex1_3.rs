// Exercise 1.3: Oneshot channel
//
// Implement a channel that carries exactly one value from a Sender to a
// Receiver. The Receiver is a future that completes when the Sender sends.
// This is a simplified version of tokio::sync::oneshot.
//
// ## Background
//
// This exercise uses the same shared-state pattern as Delay (exercise 1.2),
// but instead of a bool flag, the shared state holds an Option<T> for the
// value. The Sender stores the value and wakes the Receiver. The Receiver's
// poll() checks for the value.
//
// Two scenarios to handle:
//   - Sender sends before Receiver is polled: the value is already there on
//     the first poll, so return Ready immediately (no waker needed).
//   - Receiver is polled before Sender sends: no value yet, so store the
//     waker and return Pending. When Sender::send() runs later, it stores
//     the value and calls wake().
//
// ## What to do
//
// 1. Add fields to Inner:
//    - An Option<T> for the sent value (starts as None)
//    - An Option<Waker> for the receiver's waker (starts as None)
//    Then update the oneshot() constructor to initialize these fields.
//    Remove _phantom once you have a real T field.
//
// 2. Implement Sender::send():
//    - Lock the inner state
//    - Store the value
//    - If a waker is stored, take it and call wake()
//
// 3. Implement Receiver::poll():
//    - Lock the inner state
//    - If a value is present, take it and return Ready(value)
//    - Otherwise, store cx.waker().clone() and return Pending

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

struct Inner<T> {
    // TODO: add fields for the value and the waker.
    // Remove _phantom once you have a field that uses T.
    _phantom: std::marker::PhantomData<T>,
}

pub struct Sender<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub struct Receiver<T> {
    inner: Arc<Mutex<Inner<T>>>,
}

pub fn oneshot<T>() -> (Sender<T>, Receiver<T>) {
    // TODO: update the Inner construction once you've added fields
    let inner = Arc::new(Mutex::new(Inner {
        _phantom: std::marker::PhantomData,
    }));
    (
        Sender {
            inner: inner.clone(),
        },
        Receiver { inner },
    )
}

impl<T> Sender<T> {
    pub fn send(self, value: T) {
        // TODO: lock the inner state, store the value, wake the receiver
        // if it has registered a waker
    }
}

impl<T> Future for Receiver<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<T> {
        // TODO: lock the inner state, check for a value, store waker if absent
        todo!()
    }
}

pub fn run() {
    use std::thread;
    use std::time::Duration;

    let (tx, rx) = oneshot();
    // let (tx, rx) = crate::solutions::ex1_3::oneshot();

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(200));
        tx.send("hello from the other side");
    });
    let (val, stats) = crate::ex1_executor::run(rx);
    println!("  Result: {val}");
    println!(
        "  Polls: {}, Time: {:.0?}",
        stats.poll_count, stats.total_time
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ex1_executor;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn oneshot_from_thread() {
        let (tx, rx) = oneshot::<&str>();

        thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            tx.send("hello from the other side");
        });

        let (val, stats) = ex1_executor::run(rx);
        assert_eq!(val, "hello from the other side");
        assert_eq!(stats.poll_count, 2);
        assert!(stats.total_time >= Duration::from_millis(100));
    }

    #[test]
    fn oneshot_already_sent() {
        let (tx, rx) = oneshot::<i32>();
        tx.send(42);

        let (val, stats) = ex1_executor::run(rx);
        assert_eq!(val, 42);
        assert_eq!(
            stats.poll_count, 1,
            "Value already sent — should complete on first poll"
        );
    }
}
