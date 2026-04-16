In `new()`, spawn a thread that sleeps for the duration, then sets `complete =
true` and calls `waker.wake()` if a waker is stored.
