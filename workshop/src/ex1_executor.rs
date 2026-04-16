// Executor — provided, do not modify.
//
// This is a minimal single-future executor. It demonstrates the core execution
// loop that Tokio (and every async runtime) uses:
//
//   1. Poll the future
//   2. If Ready → done, return the result
//   3. If Pending → park the thread, wait for wake()
//   4. When woken → go to 1
//
// The key contract: when a future returns Poll::Pending, it MUST arrange for
// wake() to be called later. Otherwise the executor has no reason to poll
// again and the program hangs. This executor enforces that with a 5-second
// timeout — if nobody calls wake(), it panics.
//
// The executor also tracks stats (poll count and wall time) so the tests can
// verify that your futures behave correctly — the right number of polls means
// you're waking at the right times.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

pub struct Stats {
    pub poll_count: usize,
    pub total_time: Duration,
}

struct Signal {
    condvar: Condvar,
    notified: Mutex<bool>,
}

impl Signal {
    fn new() -> Self {
        Signal {
            condvar: Condvar::new(),
            notified: Mutex::new(false),
        }
    }

    fn wait_timeout(&self, dur: Duration) -> bool {
        let mut notified = self.notified.lock().unwrap();
        if *notified {
            *notified = false;
            return true;
        }
        let (lock, result) = self.condvar.wait_timeout(notified, dur).unwrap();
        notified = lock;
        if result.timed_out() {
            return false;
        }
        *notified = false;
        true
    }

    fn notify(&self) {
        let mut notified = self.notified.lock().unwrap();
        *notified = true;
        self.condvar.notify_one();
    }
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.notify();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.notify();
    }
}

/// Run a future to completion, parking the thread when the future is pending.
/// Returns the output and stats about execution.
pub fn run<F: Future>(mut fut: F) -> (F::Output, Stats) {
    let start = Instant::now();
    let mut poll_count = 0;

    let signal = Arc::new(Signal::new());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);

    // SAFETY: we never move `fut` after pinning it here.
    let mut fut = unsafe { Pin::new_unchecked(&mut fut) };

    loop {
        poll_count += 1;
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(output) => {
                return (
                    output,
                    Stats {
                        poll_count,
                        total_time: start.elapsed(),
                    },
                );
            }
            Poll::Pending => {
                if !signal.wait_timeout(Duration::from_secs(5)) {
                    panic!(
                        "\n\n\x1b[31mTIMEOUT: The executor waited 5 seconds and nobody \
                         called wake().\n\
                         Returning Pending without waking is a contract violation — \
                         the executor has no reason to poll again.\x1b[0m\n"
                    );
                }
            }
        }
    }
}
