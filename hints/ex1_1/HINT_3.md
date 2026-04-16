Check `self.yielded` in `poll()`. If already yielded, return `Ready`. Otherwise,
set it to `true`, call `wake_by_ref()`, and return `Pending`.
