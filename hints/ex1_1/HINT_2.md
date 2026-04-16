On the first poll, call `cx.waker().wake_by_ref()` before returning `Pending`.
This tells the executor to poll you again.
