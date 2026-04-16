`Sender::send()` should lock the inner, store the value with `inner.value =
Some(value)`, then wake the receiver if a waker is stored.
