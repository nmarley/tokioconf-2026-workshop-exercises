// Exercise 3: Mini reactor with mio
//
// Adds a reactor to the executor from exercise 2.2. The reactor maps mio
// tokens to task wakers. When the OS reports readiness on a token, the reactor
// looks up the corresponding waker and calls wake(), which pushes the task
// back onto the run queue.

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
// Reactor methods
// ============================================================================

impl Reactor {
    // Register an I/O source with mio and return its unique token.
    // Called once per socket at construction time.
    fn register(&mut self, source: &mut impl mio::event::Source, interest: Interest) -> Token {
        let token = Token(self.next_token);
        self.next_token += 1;
        self.poll
            .registry()
            .register(source, token, interest)
            .unwrap();
        token
    }

    // Store a waker for a token. I/O futures call this after getting
    // WouldBlock: "wake me when this token has events."
    fn set_waker(&mut self, token: Token, waker: std::task::Waker) {
        self.io_wakers.insert(token, waker);
    }

    // Block until I/O events arrive, then wake the corresponding tasks.
    // Each event carries a token. The reactor looks up that token in
    // io_wakers, removes the waker, and calls wake(). NOTIFY_TOKEN events
    // are skipped because they only signal that a non-I/O task was queued.
    fn turn(&mut self) {
        self.poll.poll(&mut self.events, None).unwrap();
        for event in self.events.iter() {
            if event.token() == NOTIFY_TOKEN {
                // Not an I/O event. A task was pushed to the queue via
                // spawn() or ArcWake. Nothing to dispatch.
                continue;
            }
            if let Some(waker) = self.io_wakers.remove(&event.token()) {
                waker.wake();
            }
        }
    }
}

// ============================================================================
// Provided infrastructure
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
