You need shared state between the future and a background thread. Use
`Arc<Mutex<...>>` to hold a `complete: bool` and `waker: Option<Waker>`.
