The "done" flag: create an `Arc<AtomicBool>`, clone it into a wrapper async
block around the main future. When the main future completes, the wrapper stores
`true`. The executor loop checks this flag after draining the queue.
