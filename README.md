# Workshop Exercises

## Setup

From the `exercises/` directory, run:

```bash
cargo check --workspace
```

This downloads dependencies and verifies everything compiles. Then run the exercise tests:

```bash
cargo test -p workshop-exercises
```

You should see 17 failing tests. Your goal during the workshop is to make them pass.

## Crates

This workspace contains three crates:

- `workshop`: exercises for blocks 1 and 2 (futures, executors, reactors)
- `mini-redis`: a Redis server with built-in runtime metrics (block 3 debugging exercise)
- `redis-bench`: a benchmarking tool for measuring mini-redis latency

## Exercises 1-3 (workshop crate)

Run exercises with:

```bash
cargo run -p workshop -- 1.1    # Exercise 1.1 (YieldNow)
cargo run -p workshop -- 2.2    # Exercise 2.2 (spawn)
cargo run -p workshop -- 3      # Exercise 3 (echo server)
```

Run tests with:

```bash
cargo test -p workshop           # all tests
cargo test -p workshop ex1_1     # single exercise
```

### Exercise 1: Implement Futures

Build async primitives by implementing the `Future` trait by hand.

| Level | File                    | What you build  | Real-world equivalent    |
|-------|-------------------------|-----------------|--------------------------|
|   1.1 | `workshop/src/ex1_1.rs` | YieldNow        | `tokio::task::yield_now` |
|   1.2 | `workshop/src/ex1_2.rs` | Delay           | `tokio::time::sleep`     |
|   1.3 | `workshop/src/ex1_3.rs` | Oneshot channel | `tokio::sync::oneshot`   |

Each level uses the provided executor in `workshop/src/ex1_executor.rs`.

### Exercise 2: Build an Executor

Build a single-threaded async executor from scratch.

| Level | File                    | What you build   | Real-world equivalent                |
|-------|-------------------------|------------------|--------------------------------------|
|   2.1 | `workshop/src/ex2_1.rs` | `block_on`       | `tokio::runtime::Runtime::block_on`  |
|   2.2 | `workshop/src/ex2_2.rs` | `spawn` (multi)  | `tokio::spawn`                       |

### Exercise 3: Mini Reactor

Build a reactor using mio that maps OS readiness events to task wakers, then
use it to run an async echo server.

| Level | File                            | What you build        | Real-world equivalent |
|-------|---------------------------------|-----------------------|-----------------------|
|     3 | `workshop/src/ex3.rs`           | Reactor + echo server | Tokio's I/O driver    |

```bash
# Terminal 1
cargo run -p workshop -- 3

# Terminal 2
nc 127.0.0.1 3000
```

### Hints and Solutions

Hints are in `hints/`, organized by exercise (e.g., `hints/ex1_1/`).
Each exercise has numbered hint files that progress from gentle nudges to
nearly-the-answer. Open them one at a time.

Reference solutions are in `workshop/src/solutions/`.

## Debugging Exercise (mini-redis)

The `mini-redis` crate is a Redis server with a latency problem. Your goal is
to find and fix it using Tokio's runtime metrics.

### Running the server

```bash
cargo run --release -p mini-redis
```

The server listens on port 6379. Stop it with Ctrl-C. On shutdown, the server
prints runtime metrics including the poll time histogram and per-task
scheduling delay.

### Benchmarking

Use the `redis-bench` tool to generate load and measure latency:

```bash
cargo run --release -p redis-bench
```

Default settings: 5 connections, 10,000 requests per command type, pipeline
depth 50. Override with flags:

```bash
cargo run --release -p redis-bench -- -c 10 -n 50000 -P 500
```

| Flag           | Description                                        | Default |
|----------------|----------------------------------------------------|---------|
| `-c`           | Number of parallel connections                     |       5 |
| `-n`           | Total requests per command type (SET and GET)      |  10,000 |
| `-P`           | Pipeline depth (commands in flight per connection) |      50 |
| `--value-size` | Value payload size in bytes                        |      64 |
| `--port`       | Server port                                        |    6379 |

The bench tool uses raw TCP with the Redis RESP protocol. It runs a
single-threaded mio event loop, one connection at a time in the pipeline.
Latency is measured per pipeline batch (time from write start to first
response), matching the behavior of `redis-benchmark`.

You can also use `redis-benchmark` directly:

```bash
redis-benchmark -p 6379 -t set,get -n 10000 -c 5 -P 50
```

### Using mini-redis-cli

The `mini-redis-cli` tool sends individual Redis commands to the server:

```bash
cargo run -p mini-redis --bin mini-redis-cli -- set foo bar
cargo run -p mini-redis --bin mini-redis-cli -- get foo
```

This is useful for quick manual testing or verifying that the server is
responding correctly.

### Runtime metrics

The server prints two sets of metrics on shutdown:

**Poll time histogram**: shows how long each task poll takes. Polls should
be microseconds. Polls in the hundreds-of-microseconds range indicate
synchronous work blocking the runtime.

**Connection handler task metrics**: shows per-task scheduling delay (the
gap between a task being woken and actually getting polled). High scheduling
delay means the runtime cannot get to tasks fast enough, typically because
another task is monopolizing the worker thread.

The server uses a single-threaded (`current_thread`) Tokio runtime, so all
tasks share one thread. A task that does not yield blocks every other task.

### Before the fix

Running the bench tool against the unpatched server with default settings:

```
$ cargo run --release -p redis-bench
Benchmarking 127.0.0.1:6379
  5 connections, 2000 requests each, pipeline depth 50
  Value size: 64 bytes

SET: 472628.88 requests per second, 0.02s total

Latency by percentile distribution:
    0.000% <= 0.086 milliseconds (cumulative count 1)
   50.000% <= 0.393 milliseconds (cumulative count 5000)
   75.000% <= 0.459 milliseconds (cumulative count 7500)
   93.750% <= 0.569 milliseconds (cumulative count 9375)
   99.219% <= 0.633 milliseconds (cumulative count 9921)
  100.000% <= 0.708 milliseconds (cumulative count 10000)

Summary:
  throughput: 472628.88 requests per second
  latency (msec): avg 0.391, min 0.086, p50 0.395, p95 0.589, p99 0.633, max 0.708

GET: 681092.62 requests per second, 0.01s total

Latency by percentile distribution:
    0.000% <= 0.109 milliseconds (cumulative count 1)
   50.000% <= 0.276 milliseconds (cumulative count 5000)
   75.000% <= 0.294 milliseconds (cumulative count 7500)
   93.750% <= 0.325 milliseconds (cumulative count 9375)
   99.219% <= 0.537 milliseconds (cumulative count 9921)
  100.000% <= 0.614 milliseconds (cumulative count 10000)

Summary:
  throughput: 681092.62 requests per second
  latency (msec): avg 0.277, min 0.109, p50 0.276, p95 0.326, p99 0.537, max 0.614
```

After stopping the server with Ctrl-C, the runtime metrics are printed:

```
=== Poll time histogram (421 total polls) ===

    256.00ns ..   320.00ns :        1 (  0.2%, cum   0.2%) |
    512.00ns ..   640.00ns :        1 (  0.2%, cum   1.7%) |
      2.56µs ..     3.07µs :        1 (  0.2%, cum   3.1%) |
     49.15µs ..    57.34µs :       14 (  3.3%, cum   8.3%) |###
     57.34µs ..    65.54µs :       63 ( 15.0%, cum  23.3%) |################
     65.54µs ..    81.92µs :      150 ( 35.6%, cum  58.9%) |########################################
     81.92µs ..    98.30µs :       53 ( 12.6%, cum  71.5%) |##############
     98.30µs ..   114.69µs :       48 ( 11.4%, cum  82.9%) |############
    114.69µs ..   131.07µs :       42 ( 10.0%, cum  92.9%) |###########
    131.07µs ..   163.84µs :       27 (  6.4%, cum  99.3%) |#######
    196.61µs ..   229.38µs :        2 (  0.5%, cum 100.0%) |

=== Connection handler task metrics ===

  Instrumented tasks:     10
  Dropped tasks:          10
  Total polls:            420
  Total poll duration:    34.79ms
  Mean poll duration:     82.83µs

  Total scheduled dur:    51.10ms
  Mean scheduled delay:   124.63µs
```

The poll time histogram shows 90%+ of polls take 49 to 163 microseconds. The
mean scheduling delay of 124 microseconds means tasks wait longer to be
scheduled than they spend executing. Increasing the pipeline depth or number
of connections makes this worse.

### After the fix

Running the same benchmark after applying the fix:

```
Benchmarking 127.0.0.1:6379
  5 connections, 2000 requests each, pipeline depth 50
  Value size: 64 bytes

SET: 371448.60 requests per second, 0.03s total

Latency by percentile distribution:
    0.000% <= 0.034 milliseconds (cumulative count 1)
   50.000% <= 0.065 milliseconds (cumulative count 5000)
   75.000% <= 0.079 milliseconds (cumulative count 7500)
   87.500% <= 0.093 milliseconds (cumulative count 8750)
   93.750% <= 0.110 milliseconds (cumulative count 9375)
   96.875% <= 0.151 milliseconds (cumulative count 9687)
   98.438% <= 0.182 milliseconds (cumulative count 9843)
   99.219% <= 0.240 milliseconds (cumulative count 9921)
   99.609% <= 0.245 milliseconds (cumulative count 9960)
   99.805% <= 0.245 milliseconds (cumulative count 9980)
   99.902% <= 0.245 milliseconds (cumulative count 9990)
   99.951% <= 0.245 milliseconds (cumulative count 9995)
  100.000% <= 0.245 milliseconds (cumulative count 10000)

Summary:
  throughput: 371448.60 requests per second
  latency (msec): avg 0.072, min 0.034, p50 0.066, p95 0.128, p99 0.240, max 0.245

GET: 602728.87 requests per second, 0.02s total

Latency by percentile distribution:
    0.000% <= 0.015 milliseconds (cumulative count 1)
   50.000% <= 0.033 milliseconds (cumulative count 5000)
   75.000% <= 0.040 milliseconds (cumulative count 7500)
   87.500% <= 0.045 milliseconds (cumulative count 8750)
   93.750% <= 0.063 milliseconds (cumulative count 9375)
   96.875% <= 0.075 milliseconds (cumulative count 9687)
   98.438% <= 0.081 milliseconds (cumulative count 9843)
   99.219% <= 0.119 milliseconds (cumulative count 9921)
   99.609% <= 0.130 milliseconds (cumulative count 9960)
   99.805% <= 0.130 milliseconds (cumulative count 9980)
   99.902% <= 0.130 milliseconds (cumulative count 9990)
   99.951% <= 0.130 milliseconds (cumulative count 9995)
  100.000% <= 0.130 milliseconds (cumulative count 10000)

Summary:
  throughput: 602728.87 requests per second
  latency (msec): avg 0.036, min 0.015, p50 0.034, p95 0.071, p99 0.119, max 0.130
```

SET p50 latency drops from 0.395ms to 0.066ms (6x improvement). GET p50
drops from 0.276ms to 0.034ms (8x). Tail latencies improve by a similar
factor. Throughput stays comparable because the bottleneck was latency from
scheduling delay, not raw processing speed.

The key numbers to compare:

| Metric                 | Before  | After   |
|------------------------|---------|---------|
| SET p50 latency        | 0.395ms | 0.066ms |
| GET p50 latency        | 0.276ms | 0.034ms |
| Mean poll duration     | 82.83µs |  ~8µs   |
| Mean scheduling delay  | 124.63µs| ~16µs   |

### Hints

Hints are in `hints/mini-redis/`, numbered `HINT_1.md` through `HINT_5.md`.
They progress from where to look, to what the problem is, to how to fix it.
Open them one at a time.
