You need two structs:

**Executor** — holds the run queue (`Mutex<VecDeque<Arc<Task>>>`) and a `Thread`
handle so tasks can unpark the executor.

**Task** — holds the future (`Mutex<Pin<Box<dyn Future<...>>>>`) and an
`Arc<Executor>` so it can find the queue when woken.

The `Mutex` on the future is needed because `ArcWake` requires `Send + Sync`.
