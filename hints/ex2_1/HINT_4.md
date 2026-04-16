`ArcWake::wake_by_ref` does two things: push `arc_self.clone()` onto
`arc_self.executor.queue`, and call `arc_self.executor.thread.unpark()`.
