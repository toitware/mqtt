// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import monitor
import mqtt.completion show *
import mqtt.errors show *
import mqtt.retry show *

main:
  // One terminal failure is visible to every observer, including late observers.
  receipt := Completion_
  failure := ConnectionError "READ_FAILED" "reset"
  receipt.complete --failure=failure
  3.repeat:
    expect-identical failure (catch: receipt.wait)
  receipt.complete "too late"
  expect-identical failure (catch: receipt.wait)

  // Completion wakes existing observers, not just observers arriving later.
  live := Completion_
  done := monitor.Semaphore
  3.repeat:
    task::
      expect-equals 7 live.wait
      done.up
  yield
  live.complete 7
  3.repeat: done.down

  // A waiter's deadline does not cancel the underlying operation.
  pending := Completion_
  expect-not-null (catch: with-timeout --ms=1: pending.wait)
  expect-not pending.is-complete
  pending.complete 42
  expect-equals 42 pending.wait

  // Timing tests advance timestamps, not wall-clock sleeps.
  keepalive := KeepAlive_ (Duration --s=1) 0
  expect-not (keepalive.ping-due 999_999)
  expect (keepalive.ping-due 1_000_000)
  keepalive.wrote 1_000_000 --ping
  expect-not (keepalive.ping-due 1_500_000)
  expect (keepalive.ping-expired 2_000_000)
  keepalive.received-pong
  expect-not (keepalive.ping-expired 2_000_000)
  expect-null (KeepAlive_ Duration.ZERO 0).next-deadline

  retry := RetryPolicy --attempts=3
  expect-equals (Duration --s=1) (retry.delay failure 0)
  expect-equals (Duration --s=4) (retry.delay failure 2)
  expect-null (retry.delay failure 3)
  expect-null (retry.delay (ProtocolError "bad flags") 0)
