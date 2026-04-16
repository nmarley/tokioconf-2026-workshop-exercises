The simplest fix: add `tokio::task::yield_now().await` inside the loop
in `Handler::run()`, after processing each command. This forces the task
to return `Pending` once per iteration, giving other tasks a chance to
run.

This works, but it yields on every single request, even when the server
is idle and there is only one frame to process. Can you do better?
