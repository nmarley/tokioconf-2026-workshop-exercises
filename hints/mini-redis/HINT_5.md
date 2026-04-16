A smarter approach: track whether `read_frame()` actually yielded
(returned `Poll::Pending`) or resolved immediately. If it resolves
without yielding too many times in a row (4 is a good threshold),
force a yield.

Write a small future wrapper that detects whether the inner future
ever returned `Pending` before completing:

```rust
pin_project_lite::pin_project! {
    struct YieldCheck<F> {
        #[pin]
        inner: F,
        did_yield: bool,
    }
}

impl<F: Future> Future for YieldCheck<F> {
    type Output = (F::Output, bool);

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(val) => Poll::Ready((val, *this.did_yield)),
            Poll::Pending => {
                *this.did_yield = true;
                Poll::Pending
            }
        }
    }
}
```

Then wrap `read_frame()` with it inside the loop. If `did_yield` is
true, the runtime already had a chance to schedule other tasks, so
reset the counter. If false, increment it. When the counter hits the
threshold, call `yield_now().await` and reset.

```rust
let mut consecutive_no_yield: u32 = 0;

while !self.shutdown.is_shutdown() {
    let (maybe_frame, did_yield) = tokio::select! {
        res = yield_check(self.connection.read_frame()) => {
            let (result, did_yield) = res;
            (result?, did_yield)
        }
        _ = self.shutdown.recv() => return Ok(()),
    };

    let frame = match maybe_frame {
        Some(frame) => frame,
        None => return Ok(()),
    };

    let cmd = Command::from_frame(frame)?;
    cmd.apply(&self.db, &mut self.connection, &mut self.shutdown)
        .await?;

    if did_yield {
        consecutive_no_yield = 0;
    } else {
        consecutive_no_yield += 1;
        if consecutive_no_yield >= 4 {
            consecutive_no_yield = 0;
            tokio::task::yield_now().await;
        }
    }
}
```

This avoids unnecessary yields when the server is idle. The task only
forces a yield when `read_frame()` keeps resolving immediately,
meaning the socket buffer has data queued up and the loop would
otherwise spin without giving other tasks a chance to run.
