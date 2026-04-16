use futures::task::ArcWake;
use std::cell::Cell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;
use std::thread::{self, Thread};

struct Executor {
    queue: Mutex<VecDeque<Arc<Task>>>,
    thread: Thread,
}

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    executor: Arc<Executor>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self
            .executor
            .queue
            .lock()
            .unwrap()
            .push_back(arc_self.clone());
        arc_self.executor.thread.unpark();
    }
}

thread_local! {
    static CURRENT: Cell<Option<Arc<Executor>>> = const { Cell::new(None) };
}

pub fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    CURRENT.with(|current| {
        let executor = current
            .take()
            .expect("spawn() called outside of block_on — no executor is running");

        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(fut)),
            executor: executor.clone(),
        });

        executor.queue.lock().unwrap().push_back(task);
        executor.thread.unpark();

        // Put it back
        current.set(Some(executor));
    });
}

pub fn block_on(fut: impl Future<Output = ()> + Send + 'static) {
    let executor = Arc::new(Executor {
        queue: Mutex::new(VecDeque::new()),
        thread: thread::current(),
    });

    // Set the thread-local so spawn() works
    CURRENT.with(|current| current.set(Some(executor.clone())));

    // Wrap the main future to set a done flag when it completes
    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();

    let main_task = Arc::new(Task {
        future: Mutex::new(Box::pin(async move {
            fut.await;
            done_clone.store(true, Ordering::Release);
        })),
        executor: executor.clone(),
    });

    executor.queue.lock().unwrap().push_back(main_task);

    loop {
        // Drain the queue
        loop {
            let task = executor.queue.lock().unwrap().pop_front();
            let Some(task) = task else { break };

            let waker = futures::task::waker(task.clone());
            let mut cx = Context::from_waker(&waker);
            let mut future = task.future.lock().unwrap();

            // Poll the task — if it's done, just drop it (don't return)
            let _ = future.as_mut().poll(&mut cx);
        }

        // Check if the main future completed
        if done.load(Ordering::Acquire) {
            // Clean up the thread-local
            CURRENT.with(|current| current.set(None));
            return;
        }

        // Queue is empty but main future isn't done — stuck!
        if executor.queue.lock().unwrap().is_empty() {
            CURRENT.with(|current| current.set(None));
            panic!(
                "no runnable tasks but main future hasn't completed — \
                 a task likely returned Pending without calling wake()"
            );
        }

        // Park until a waker fires
        thread::park();
    }
}
