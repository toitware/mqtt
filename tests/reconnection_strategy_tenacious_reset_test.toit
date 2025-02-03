// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker-internal
import .broker-mosquitto
import .packet-test-client
import .transport

/**
Tests the tenacious reconnection strategy.

The reconnection strategy keeps on trying.
The only way to stop it is to close the client.
*/
test create-transport/Lambda --logger/log.Logger:
  failing-transport /CallbackTestTransport? := null

  create-failing-transport := ::
    transport := create-transport.call
    failing-transport = CallbackTestTransport transport
    failing-transport

  delay-lambda-client := null  // Will be set later.
  delay-lambda-should-throw := false
  delay-lambda-should-close := false
  delay-lambda-semaphore := monitor.Semaphore
  delay-lambda-called-semaphore := monitor.Semaphore
  reconnection-strategy := mqtt.TenaciousReconnectionStrategy --logger=logger
      --reset-duration=Duration --ms=500
      --delay-lambda=:: Duration --ms=500

  with-packet-client create-failing-transport
      --client-id = "tenacious-client"
      --no-clean-session
      --reconnection-strategy = reconnection-strategy
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    is-write-failing := false

    reconnection-attempts := 0
    reconnect-latch := monitor.Latch
    failing-transport.on-reconnect = ::
      is-write-failing = false
      reconnection-attempts++
      if not reconnect-latch.has-value: reconnect-latch.set true

    failing-transport.on-write = ::
      if is-write-failing: throw "failing"

    wait-for-idle.call
    clear.call

    // Will be reset to false in the reconnection attempt.
    is-write-failing = true

    before-publish := Time.monotonic-us

    published-latch := monitor.Latch
    task::
      client.publish "first-failing. then working." #[] --qos=0
      print "published"
      published-latch.set true

    // Since we just connected a few moments ago the client isn't allowed to
    // connect yet.
    expect-equals 0 reconnection-attempts

    published-latch.get

    after-publish := Time.monotonic-us

    expect (after-publish - before-publish) > (Duration --ms=500).in-us
    // Now we should have connected again.
    expect-equals 1 reconnection-attempts

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  if test-with-mosquitto: return

  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  with-internal-broker --logger=logger run-test
