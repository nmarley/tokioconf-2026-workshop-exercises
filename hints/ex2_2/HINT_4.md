After draining the queue, check three things in order:

1. Is `done` set? -> return (success)
2. Is the queue empty? -> panic ("no runnable tasks but main future hasn't
   completed")
3. Otherwise -> `thread::park()` (wait for a waker)
