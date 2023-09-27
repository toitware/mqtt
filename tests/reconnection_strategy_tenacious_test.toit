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

import .broker_internal
import .broker_mosquitto
import .packet_test_client
import .transport

/**
Tests the tenacious reconnection strategy.

The reconnection strategy keeps on trying.
The only way to stop it is to close the client.
*/
test create_transport/Lambda --logger/log.Logger:
  failing_transport /CallbackTestTransport? := null

  create_failing_transport := ::
    transport := create_transport.call
    failing_transport = CallbackTestTransport transport
    failing_transport

  delay_lambda_client := null  // Will be set later.
  delay_lambda_should_throw := false
  delay_lambda_should_close := false
  delay_lambda_semaphore := monitor.Semaphore
  delay_lambda_called_semaphore := monitor.Semaphore
  reconnection_strategy := mqtt.TenaciousReconnectionStrategy --logger=logger
      --delay_lambda=::
        delay_lambda_called_semaphore.up
        delay_lambda_semaphore.down
        if delay_lambda_should_throw:
          throw "delay_lambda_should_throw"
        if delay_lambda_should_close:
          delay_lambda_client.close
        Duration.ZERO

  with_packet_client create_failing_transport
      --client_id = "tenacious-client"
      --no-clean_session
      --reconnection_strategy = reconnection_strategy
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    reconnection_attempts := 0
    is_reconnect_failing := false
    failing_transport.on_reconnect = ::
      reconnection_attempts++
      if is_reconnect_failing: throw "failing"

    is_write_failing := false
    failing_transport.on_write = ::
      if is_write_failing: throw "failing"

    wait_for_idle.call
    clear.call

    // Destroy the transport. From the client's side it looks as if all writes fail from now on.
    is_write_failing = true
    is_reconnect_failing = true

    task::
      // This packet will not make it through initially as the transport is dead.
      // However, the tenacious reconnection strategy will keep on trying, and
      // eventually the packet will be sent.
      client.publish "eventually_succeeding" #[] --qos=0

    was_disconnected := false
    failing_transport.on_disconnect = ::
      was_disconnected = true

      // Allow for 10 reconnection attempts.
    10.repeat: delay_lambda_semaphore.up
    10.repeat: delay_lambda_called_semaphore.down

    expect was_disconnected

    // Switch back to just having the write fail.
    is_reconnect_failing = false
    10.repeat: delay_lambda_semaphore.up
    10.repeat: delay_lambda_called_semaphore.down

    // Finally switch back to a working transport.
    is_write_failing = false

    delay_lambda_semaphore.up

    wait_for_idle.call

    activity := get_activity.call

    expect_equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).size

    clear.call

    // We increased the delay_lambda_semaphore too much (just in case).
    // Make sure we are back to 0.
    while delay_lambda_semaphore.count != 0: delay_lambda_semaphore.down

    // Try to throw in the delay lambda.
    reconnection_attempts = 0
    delay_lambda_should_throw = true
    is_write_failing = true

    task::
      client.publish "eventually_succeeding" #[] --qos=0

    10.repeat: delay_lambda_semaphore.up
    10.repeat: delay_lambda_called_semaphore.down

    // Since the delay lambda failed we should only have one reconnection attempt. (The
    // first one that isn't delayed).
    expect_equals 1 reconnection_attempts

    delay_lambda_should_throw = false
    is_write_failing = false

    delay_lambda_semaphore.up

    wait_for_idle.call

    // We increased the delay_lambda_semaphore too much (just in case).
    // Make sure we are back to 0.
    while delay_lambda_semaphore.count != 0: delay_lambda_semaphore.down

    // Try to close in the delay lambda.
    delay_lambda_client = client
    delay_lambda_should_close = true
    is_write_failing = true

    failed_latch := monitor.Latch
    task::
      exception := catch: client.publish "failing" #[] --qos=0
      expect_not_null exception
      failed_latch.set "done"

    delay_lambda_semaphore.up
    failed_latch.get

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  if test_with_mosquitto: return

  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
