// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
An operation's persistent result, observable by any number of tasks.

A timeout or cancellation while waiting does not cancel delivery. MQTT may already
  have delivered a message even when its acknowledgement has not arrived.
*/
interface Receipt:
  wait -> any
  is-complete -> bool

/** The owner's writable end of a receipt. */
monitor Completion_ implements Receipt:
  done_/bool := false
  value_/any := null
  failure_/any := null

  /** Waits for completion, returning the result or throwing its failure. */
  wait -> any:
    await: done_
    if failure_: throw failure_
    return value_

  /** Whether this operation has completed. */
  is-complete -> bool:
    return done_

  complete value=null --failure=null -> none:
    if done_: return
    value_ = value
    failure_ = failure
    done_ = true
