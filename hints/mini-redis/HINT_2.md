The poll time histogram shows polls in the hundreds-of-microseconds range. That means a single `poll()` call on a task is
running for a long time without returning `Poll::Pending`. Since the
runtime is single-threaded, no other task can make progress while this
is happening.

Which task is responsible? Check the connection handler task metrics
printed on shutdown. The `TaskMonitor` tracks mean poll duration per
task group, which tells you whether the connection handlers are the
source of the long polls.
