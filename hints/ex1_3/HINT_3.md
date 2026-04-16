`Receiver::poll()` should lock the inner. If a value is present, take it with
`inner.value.take()` and return `Ready(value)`. Otherwise, store the waker and
return `Pending`.
