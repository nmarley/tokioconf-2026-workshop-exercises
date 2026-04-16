use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::thread;
use std::time::Duration;

struct DelayState {
    complete: bool,
    waker: Option<Waker>,
}

pub struct Delay {
    state: Arc<Mutex<DelayState>>,
}

impl Delay {
    pub fn new(dur: Duration) -> Self {
        let state = Arc::new(Mutex::new(DelayState {
            complete: false,
            waker: None,
        }));

        let thread_state = state.clone();
        thread::spawn(move || {
            thread::sleep(dur);
            let mut state = thread_state.lock().unwrap();
            state.complete = true;
            if let Some(waker) = state.waker.take() {
                waker.wake();
            }
        });

        Delay { state }
    }
}

impl Future for Delay {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().unwrap();
        if state.complete {
            Poll::Ready(())
        } else {
            state.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}
