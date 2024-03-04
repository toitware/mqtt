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
      --delay-lambda=::
        delay-lambda-called-semaphore.up
        delay-lambda-semaphore.down
        if delay-lambda-should-throw:
          throw "delay_lambda_should_throw"
        if delay-lambda-should-close:
          delay-lambda-client.close
        Duration.ZERO

  with-packet-client create-failing-transport
      --client-id = "tenacious-client"
      --no-clean-session
      --reconnection-strategy = reconnection-strategy
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    reconnection-attempts := 0
    is-reconnect-failing := false
    failing-transport.on-reconnect = ::
      reconnection-attempts++
      if is-reconnect-failing: throw "failing"

    is-write-failing := false
    failing-transport.on-write = ::
      if is-write-failing: throw "failing"

    wait-for-idle.call
    clear.call

    // Destroy the transport. From the client's side it looks as if all writes fail from now on.
    is-write-failing = true
    is-reconnect-failing = true

    task::
      // This packet will not make it through initially as the transport is dead.
      // However, the tenacious reconnection strategy will keep on trying, and
      // eventually the packet will be sent.
      client.publish "eventually_succeeding" #[] --qos=0

    was-disconnected := false
    failing-transport.on-disconnect = ::
      was-disconnected = true

      // Allow for 10 reconnection attempts.
    10.repeat: delay-lambda-semaphore.up
    10.repeat: delay-lambda-called-semaphore.down

    expect was-disconnected

    // Switch back to just having the write fail.
    is-reconnect-failing = false
    10.repeat: delay-lambda-semaphore.up
    10.repeat: delay-lambda-called-semaphore.down

    // Finally switch back to a working transport.
    is-write-failing = false

    delay-lambda-semaphore.up

    wait-for-idle.call

    activity := get-activity.call

    expect-equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).size

    clear.call

    // We increased the delay_lambda_semaphore too much (just in case).
    // Make sure we are back to 0.
    while delay-lambda-semaphore.count != 0: delay-lambda-semaphore.down

    // Try to throw in the delay lambda.
    reconnection-attempts = 0
    delay-lambda-should-throw = true
    is-write-failing = true

    task::
      client.publish "eventually_succeeding" #[] --qos=0

    10.repeat: delay-lambda-semaphore.up
    10.repeat: delay-lambda-called-semaphore.down

    // Since the delay lambda failed we should only have one reconnection attempt. (The
    // first one that isn't delayed).
    expect-equals 1 reconnection-attempts

    delay-lambda-should-throw = false
    is-write-failing = false

    delay-lambda-semaphore.up

    wait-for-idle.call

    // We increased the delay_lambda_semaphore too much (just in case).
    // Make sure we are back to 0.
    while delay-lambda-semaphore.count != 0: delay-lambda-semaphore.down

    // Try to close in the delay lambda.
    delay-lambda-client = client
    delay-lambda-should-close = true
    is-write-failing = true

    failed-latch := monitor.Latch
    task::
      exception := catch: client.publish "failing" #[] --qos=0
      expect-not-null exception
      failed-latch.set "done"

    delay-lambda-semaphore.up
    failed-latch.get

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  if test-with-mosquitto: return

  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  with-internal-broker --logger=logger run-test
