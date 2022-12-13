// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor // For toitdoc.
import writer // For toitdoc.

/*
A (trimmed down) copy of $writer.Writer.

Older version of the SDK require the `writer_` to have `write data from to`.
That doesn't work with our Transport class, so we use a copy of the newer
  version here.
*/
// TODO(florian): remove copy of the Writer.
class Writer:
  writer_ := ?

  constructor .writer_:

  write data/ByteArray:
    size := data.size
    written := 0
    while written < size:
      written += writer_.write data[written..]
      if written != size: yield
    return size

  close -> none:
    writer_.close

/**
A copy of $monitor.Latch.

Older version of the SDK don't have the $has_value function, so
  we use a copy here.
*/
// TODO(florian): remove copy of the Latch and use the original one
// from the monitor library.
monitor Latch:
  has_value_ := false
  value_ := null

  get:
    await: has_value_
    return value_

  set value:
    value_ = value
    has_value_ = true

  has_value -> bool:
    return has_value_

/**
A signal synchronization primitive.

# Inheritance
This class must not be extended.
*/
monitor Signal:
  current_ /int := 0
  awaited_ /int := 0

  /**
  Waits until the signal has been raised.

  Raises that occur before $wait has been called are not taken into
    account, so care must be taken to avoid losing information.
  */
  wait -> none:
    wait_: true

  /**
  Waits until the given $condition returns true.

  The $condition is evaluated on entry.

  This task is blocked until the $condition returns true.

  The condition is re-evaluated (on this task) whenever the signal has been raised.
  */
  wait [condition] -> none:
    if condition.call: return
    wait_ condition

  /**
  Raises the signal and unblocks the tasks that are already waiting.

  If $max is provided and not null, no more than $max tasks are
    woken up in the order in which they started waiting (FIFO).
    The most common use case is to wake waiters up one at a time.
  */
  raise --max/int?=null -> none:
    if max:
      if max < 1: throw "INVALID_ARGUMENT"
      current_ = min awaited_ (current_ + max)
    else:
      current_ = awaited_

  // Helper method for condition waiting.
  wait_ [condition] -> none:
    while true:
      awaited := awaited_
      if current_ == awaited:
        // No other task is waiting for this signal to be raised,
        // so it is safe to reset the counters. This helps avoid
        // the ever increasing counter issue that may lead to poor
        // performance in (very) extreme cases.
        current_ = awaited = 0
      awaited_ = ++awaited
      await: current_ >= awaited
      if condition.call: return

/**
A semaphore synchronization primitive.

# Inheritance
This class must not be extended.
*/
monitor Semaphore:
  count_ /int := ?
  limit_ /int?

  /**
  Constructs a semaphore with an initial $count and an optional $limit.

  When the $limit is reached, further attempts to increment the
    counter using $up are ignored and leaves the counter unchanged.
  */
  constructor --count/int=0 --limit/int?=null:
    if count < 0: throw "INVALID_ARGUMENT"
    if limit and (limit < 1 or count > limit): throw "INVALID_ARGUMENT"
    limit_ = limit
    count_ = count

  /**
  Increments an internal counter.

  Originally called the V operation.
  */
  up -> none:
    count := count_
    limit := limit_
    if limit and count >= limit: return
    count_ = count + 1

  /**
  Decrements an internal counter.
  This method blocks until the counter is non-zero.

  Originally called the P operation.
  */
  down -> none:
    await: count_ > 0
    count_--

  /** The current count of the semaphore. */
  count -> int:
    return count_
