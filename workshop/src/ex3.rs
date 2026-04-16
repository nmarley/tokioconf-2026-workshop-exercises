// Exercise 3: Mini reactor with mio
//
// ## Background
//
// The executor from exercise 2.2 parks the thread (thread::park) when no tasks
// are runnable. That works for timer-based futures (where a background thread
// calls wake()), but not for I/O. For networking, the OS must tell us when a
// socket is ready to read or write. That is the reactor's job.
//
// The reactor wraps mio::Poll, which is a cross-platform interface to the OS's
// I/O event system (epoll on Linux, kqueue on macOS). The flow:
//
//   1. An I/O future (e.g. TcpStream::read) tries a non-blocking operation.
//   2. If the operation returns WouldBlock, the future stores its waker in the
//      reactor (keyed by a unique mio Token) and returns Pending.
//   3. When the executor has no runnable tasks, it calls reactor.turn(), which
//      blocks in mio::Poll::poll() until the OS reports I/O events.
//   4. For each event, the reactor looks up the waker by token and calls wake().
//   5. The executor polls the re-woken task, which retries the I/O operation.
//
// ## What to implement
//
// You implement three Reactor methods:
//   - register: assign a unique mio Token to a socket and register it with mio
//   - set_waker: store a waker for a token (called by I/O futures on WouldBlock)
//   - turn: block in mio::Poll::poll(), then wake tasks for each event
//
// The executor, I/O types (TcpListener, TcpStream), and echo server are
// provided below. Read through them to see how they call your three methods.
//
// When you're done, run `cargo run -- 3` and test with `nc 127.0.0.1 3000`.
//
// See hints/ex3/ if you get stuck.

use futures::task::ArcWake;
use mio::net::{TcpListener as MioListener, TcpStream as MioStream};
use mio::{Events, Interest, Poll, Token};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Context;

// ============================================================================
// Reactor
//
// Owns the mio::Poll and maintains the token-to-waker map. When the executor
// has no runnable tasks, it calls reactor.turn(), which blocks in
// mio::Poll::poll() until I/O events arrive. For each event, the reactor
// looks up the waker by token and calls wake().
// ============================================================================

struct Reactor {
    poll: Poll,
    events: Events,

    // Maps mio tokens to task wakers. When an I/O event fires for a token,
    // turn() removes the waker and calls wake().
    io_wakers: HashMap<Token, std::task::Waker>,

    // Monotonic counter for assigning unique tokens to I/O sources.
    next_token: usize,
}

impl Reactor {
    fn new(poll: Poll) -> Self {
        Reactor {
            poll,
            events: Events::with_capacity(64),
            io_wakers: HashMap::new(),
            next_token: 0,
        }
    }
}

// ============================================================================
// YOUR WORK: implement these three methods
// ============================================================================

impl Reactor {
    // TODO: Implement register.
    //
    // Each I/O source (socket) needs a unique mio Token so the reactor can
    // tell which socket an event belongs to.
    //
    // Steps:
    //   1. Create a Token from self.next_token
    //   2. Increment self.next_token
    //   3. Call self.poll.registry().register(source, token, interest)
    //      to tell mio to watch this socket. Unwrap the result.
    //   4. Return the token
    fn register(
        &mut self,
        source: &mut impl mio::event::Source,
        interest: Interest,
    ) -> Token {
        let _ = (source, interest);
        todo!("implement register")
    }

    // TODO: Implement set_waker.
    //
    // Store the waker in self.io_wakers keyed by the token. I/O futures call
    // this after getting WouldBlock, so when the OS reports that token is
    // ready, turn() can find and call the right waker.
    //
    // This is one line: insert into the HashMap.
    fn set_waker(&mut self, token: Token, waker: std::task::Waker) {
        let _ = (token, waker);
        todo!("implement set_waker")
    }

    // TODO: Implement turn.
    //
    // This is where the reactor blocks, waiting for the OS to report I/O
    // events, then wakes the corresponding tasks.
    //
    // Steps:
    //   1. Call self.poll.poll(&mut self.events, None) to block until events
    //      arrive. The None timeout means "wait forever." Unwrap the result.
    //   2. Iterate over &self.events. For each event:
    //      - Get the event's token with event.token()
    //      - Skip events where token == NOTIFY_TOKEN. These come from the
    //        mio::Waker and mean "a task was queued via spawn() or ArcWake."
    //        The executor handles those by draining the queue, not via wakers.
    //      - For all other tokens, remove the waker from self.io_wakers and
    //        call wake() on it. (Use .remove(), not .get(), so the waker is
    //        consumed. The I/O future will re-register a new waker if needed.)
    fn turn(&mut self) {
        todo!("implement turn")
    }
}

// ============================================================================
// Everything below is provided. You should not need to modify it.
// ============================================================================

// Reserved token for the mio::Waker. Events with this token mean "a task was
// woken outside of I/O": the executor should check the run queue.
const NOTIFY_TOKEN: Token = Token(usize::MAX);

// ============================================================================
// Executor (adapted from exercise 2.2)
// ============================================================================

struct Executor {
    queue: Mutex<VecDeque<Arc<Task>>>,
    // Breaks the executor out of mio::Poll::poll() when a task is queued from
    // outside the I/O event loop.
    notify: mio::Waker,
}

struct Task {
    future: Mutex<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    executor: Arc<Executor>,
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self
            .executor
            .queue
            .lock()
            .unwrap()
            .push_back(arc_self.clone());
        // Signal the mio::Poll so the executor loop wakes up and drains
        // the queue.
        let _ = arc_self.executor.notify.wake();
    }
}

// ============================================================================
// Thread-locals
// ============================================================================

thread_local! {
    // The current executor, so spawn() can find the queue.
    static EXECUTOR: Cell<Option<Arc<Executor>>> = const { Cell::new(None) };

    // The reactor, so I/O types can register sources and store wakers.
    // RefCell because the reactor needs &mut access (single-threaded only).
    static REACTOR: RefCell<Option<Reactor>> = const { RefCell::new(None) };
}

// Access the thread-local reactor. I/O types use this to register sources
// and store wakers.
fn with_reactor<F, R>(f: F) -> R
where
    F: FnOnce(&mut Reactor) -> R,
{
    REACTOR.with(|r| f(r.borrow_mut().as_mut().expect("no reactor running")))
}

// ============================================================================
// Public API: spawn and block_on
// ============================================================================

// Wrap the future in a Task and push it onto the run queue.
pub fn spawn(fut: impl Future<Output = ()> + Send + 'static) {
    EXECUTOR.with(|current| {
        let executor = current
            .take()
            .expect("spawn() called outside of block_on()");
        let task = Arc::new(Task {
            future: Mutex::new(Box::pin(fut)),
            executor: executor.clone(),
        });
        executor.queue.lock().unwrap().push_back(task);
        let _ = executor.notify.wake();
        current.set(Some(executor));
    });
}

pub fn block_on(fut: impl Future<Output = ()> + Send + 'static) {
    // The Poll is shared between the Executor (via mio::Waker) and the
    // Reactor. The Waker lets ArcWake signal "new task in queue" from any
    // context, breaking the reactor out of its blocking poll() call.
    let poll = Poll::new().unwrap();
    let notify = mio::Waker::new(poll.registry(), NOTIFY_TOKEN).unwrap();

    let executor = Arc::new(Executor {
        queue: Mutex::new(VecDeque::new()),
        notify,
    });

    let reactor = Reactor::new(poll);

    // Store both in thread-locals so spawn() and I/O types can find them.
    EXECUTOR.with(|c| c.set(Some(executor.clone())));
    REACTOR.with(|r| *r.borrow_mut() = Some(reactor));

    // Wrap the main future so we can detect when it completes, separately
    // from spawned tasks.
    let done = Arc::new(AtomicBool::new(false));
    let done_clone = done.clone();

    let main_task = Arc::new(Task {
        future: Mutex::new(Box::pin(async move {
            fut.await;
            done_clone.store(true, Ordering::Release);
        })),
        executor: executor.clone(),
    });
    executor.queue.lock().unwrap().push_back(main_task);

    loop {
        // Drain the task queue.
        loop {
            let task = executor.queue.lock().unwrap().pop_front();
            let Some(task) = task else { break };
            let waker = futures::task::waker(task.clone());
            let mut cx = Context::from_waker(&waker);
            let _ = task.future.lock().unwrap().as_mut().poll(&mut cx);
        }

        // Check if the main future completed.
        if done.load(Ordering::Acquire) {
            EXECUTOR.with(|c| c.set(None));
            REACTOR.with(|r| *r.borrow_mut() = None);
            return;
        }

        // Block in mio::Poll::poll(), waiting for I/O events (which wake
        // tasks via the token-to-waker map) or an mio::Waker signal (which
        // means a non-I/O task was queued).
        with_reactor(|r| r.turn());
    }
}

// ============================================================================
// I/O types
//
// Thin wrappers around mio's networking types. Each registers itself with the
// reactor on creation. Async methods follow a common pattern:
//
//   1. Try the operation (accept, read, write).
//   2. On success, return Ready with the result.
//   3. On WouldBlock, store the task's waker in the reactor and return
//      Pending. The reactor calls wake() when the OS reports events on
//      this socket.
//   4. The executor polls the task again, which retries from step 1.
// ============================================================================

pub struct TcpListener {
    inner: MioListener,
    token: Token,
}

impl TcpListener {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let mut inner = MioListener::bind(addr)?;
        // Register with the reactor for readiness notifications on incoming
        // connections.
        let token = with_reactor(|r| r.register(&mut inner, Interest::READABLE));
        Ok(TcpListener { inner, token })
    }

    pub async fn accept(&self) -> io::Result<TcpStream> {
        std::future::poll_fn(|cx| {
            // mio's accept() is non-blocking.
            match self.inner.accept() {
                Ok((mut stream, _addr)) => {
                    // Register the new stream for both read and write events.
                    let token = with_reactor(|r| {
                        r.register(&mut stream, Interest::READABLE | Interest::WRITABLE)
                    });
                    std::task::Poll::Ready(Ok(TcpStream {
                        inner: stream,
                        token,
                    }))
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No pending connections. Store waker so the reactor
                    // wakes us when one arrives.
                    with_reactor(|r| r.set_waker(self.token, cx.waker().clone()));
                    std::task::Poll::Pending
                }
                Err(e) => std::task::Poll::Ready(Err(e)),
            }
        })
        .await
    }
}

pub struct TcpStream {
    inner: MioStream,
    token: Token,
}

impl TcpStream {
    pub async fn read(&self, buf: &mut [u8]) -> io::Result<usize> {
        std::future::poll_fn(|cx| {
            // `Read for &MioStream` lets us call read with &self.
            match (&self.inner).read(buf) {
                Ok(n) => std::task::Poll::Ready(Ok(n)),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No data available. Store waker so the reactor wakes
                    // us when data arrives.
                    with_reactor(|r| r.set_waker(self.token, cx.waker().clone()));
                    std::task::Poll::Pending
                }
                Err(e) => std::task::Poll::Ready(Err(e)),
            }
        })
        .await
    }

    pub async fn write_all(&self, buf: &[u8]) -> io::Result<()> {
        let mut pos = 0;
        while pos < buf.len() {
            let n = std::future::poll_fn(|cx| {
                // `Write for &MioStream` lets us call write with &self.
                match (&self.inner).write(&buf[pos..]) {
                    Ok(n) => std::task::Poll::Ready(Ok(n)),
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // Socket buffer full. Store waker so the reactor
                        // wakes us when the socket is writable again.
                        with_reactor(|r| r.set_waker(self.token, cx.waker().clone()));
                        std::task::Poll::Pending
                    }
                    Err(e) => std::task::Poll::Ready(Err(e)),
                }
            })
            .await?;
            pos += n;
        }
        Ok(())
    }
}

// ============================================================================
// Echo server
//
// Accepts connections in a loop and spawns a task per connection. Each task
// reads data and writes it back until the client disconnects.
// ============================================================================

pub fn run() {
    println!("=== Exercise 3: Mini Reactor ===\n");
    let addr: SocketAddr = "127.0.0.1:3000".parse().unwrap();
    block_on(echo_server(addr));
}

pub async fn echo_server(addr: SocketAddr) {
    let listener = TcpListener::bind(addr).unwrap();
    println!("listening on {addr}");
    println!("connect with: nc 127.0.0.1 3000\n");

    loop {
        let stream = listener.accept().await.unwrap();
        spawn(async move {
            if let Err(e) = echo(stream).await {
                eprintln!("connection error: {e}");
            }
        });
    }
}

async fn echo(stream: TcpStream) -> io::Result<()> {
    let mut buf = [0u8; 1024];
    loop {
        let n = stream.read(&mut buf).await?;
        if n == 0 {
            // Client closed the connection.
            return Ok(());
        }
        stream.write_all(&buf[..n]).await?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpStream as StdStream;

    #[test]
    fn echo_server_test() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

        // Bind first to get the actual port, then drop so the server can rebind.
        let listener = std::net::TcpListener::bind(addr).unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        // Run the echo server in a background thread.
        let handle = std::thread::spawn(move || {
            block_on(async move {
                let listener = TcpListener::bind(addr).unwrap();
                // Accept one connection, echo it, then exit.
                let stream = listener.accept().await.unwrap();
                echo(stream).await.unwrap();
            });
        });

        // Give the server a moment to start.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Connect with a std TcpStream and verify echo.
        let mut client = StdStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();

        let msg = b"hello world";
        client.write_all(msg).unwrap();

        let mut buf = [0u8; 64];
        let n = client.read(&mut buf).unwrap();
        assert_eq!(&buf[..n], msg);

        // Close the connection so the server's echo() returns and block_on exits.
        drop(client);
        handle.join().unwrap();
    }
}
