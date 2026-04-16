Each poll of a connection handler task calls `read_frame()`, processes
the command, and writes a response. If `read_frame()` returns a frame
immediately (because the socket buffer already has data), the task
processes it, writes the response, and loops back to `read_frame()`
without ever returning `Pending`.

Under high pipeline depth, the client sends many commands before waiting
for responses. The socket's receive buffer fills up. The task keeps
reading and processing frames in a tight loop, never yielding back to
the runtime.
