In `block_on`: create the `Executor` with `thread::current()`, wrap `fut` in a
`Task` with `Box::pin`, and push the task onto the queue. Then enter the loop.
