In `poll()`, lock the state. If `complete`, return `Ready`. Otherwise, store
`cx.waker().clone()` and return `Pending`.
