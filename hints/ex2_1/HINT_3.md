The executor loop: pop a task from the queue (releasing the lock first!), create
a waker with `futures::task::waker(task.clone())`, build a `Context`, and poll
the future. If `Ready`, return. If `Pending`, the waker will re-enqueue the task
later. When the queue is empty, call `thread::park()`.
