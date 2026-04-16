use futures::task::ArcWake;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
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

pub fn block_on(fut: impl Future<Output = ()> + Send + 'static) {
    let executor = Arc::new(Executor {
        queue: Mutex::new(VecDeque::new()),
        thread: thread::current(),
    });

    // Wrap the future in a Task and enqueue it
    let task = Arc::new(Task {
        future: Mutex::new(Box::pin(fut)),
        executor: executor.clone(),
    });
    executor.queue.lock().unwrap().push_back(task);

    loop {
        // Drain the queue
        loop {
            let task = executor.queue.lock().unwrap().pop_front();
            let Some(task) = task else { break };

            let waker = futures::task::waker(task.clone());
            let mut cx = Context::from_waker(&waker);
            let mut future = task.future.lock().unwrap();

            if future.as_mut().poll(&mut cx).is_ready() {
                return;
            }
        }

        // Nothing to do — park until a waker fires
        thread::park();
    }
}