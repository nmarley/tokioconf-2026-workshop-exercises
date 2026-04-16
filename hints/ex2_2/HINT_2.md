Define the thread-local with `thread_local!` and `Cell<Option<Arc<Executor>>>`.

In `spawn()`, use `take()` to get the executor out of the `Cell`, use it, then
`set()` it back. This avoids borrowing issues.

In `block_on()`, set the thread-local before the loop and clear it when done.
