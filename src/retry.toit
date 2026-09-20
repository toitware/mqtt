// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .errors

/** A bounded exponential delay policy with optional unlimited attempts. */
class RetryPolicy:
  attempts/int
  initial-delay/Duration
  maximum-delay/Duration
  stable-after/Duration

  constructor --.attempts=-1
      --.initial-delay=(Duration --s=1)
      --.maximum-delay=(Duration --s=30)
      --.stable-after=(Duration --s=30):
    if attempts < -1 or initial-delay.in-us < 0 or maximum-delay < initial-delay or stable-after.in-us < 0:
      throw "INVALID_ARGUMENT"

  /** Returns the next delay, or null if this failure is terminal. */
  delay failure/any attempt/int -> Duration?:
    if failure is not ConnectionError: return null
    if attempts >= 0 and attempt >= attempts: return null
    factor := 1 << (min attempt 30)
    return Duration --us=(min maximum-delay.in-us (initial-delay.in-us * factor))

/** Tracks keepalive using timestamps; it performs no I/O and owns no task. */
class KeepAlive_:
  interval_/int
  last-write_/int := ?
  ping-deadline_/int? := null

  constructor duration/Duration now/int:
    interval_ = duration.in-us
    last-write_ = now

  next-deadline -> int?:
    if interval_ == 0: return null
    return ping-deadline_ or (last-write_ + interval_)

  ping-expired now/int -> bool:
    return ping-deadline_ != null and now >= ping-deadline_

  ping-due now/int -> bool:
    return interval_ != 0 and not ping-deadline_ and now >= last-write_ + interval_

  wrote now/int --ping/bool=false:
    last-write_ = now
    if ping: ping-deadline_ = now + interval_

  received-pong:
    ping-deadline_ = null
