// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker_internal
import .broker_mosquitto
import .packet_test_client
import .transport
import .util

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  if test_with_mosquitto: return

  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test


class TestTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport

  on_reconnect /Lambda? := null
  on_write /Lambda? := null
  on_read /Lambda? := null

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    if on_write: on_write.call bytes
    return wrapped_.write bytes

  read -> ByteArray?:
    if on_read: return on_read.call wrapped_
    return wrapped_.read

  close -> none: wrapped_.close
  supports_reconnect -> bool: return wrapped_.supports_reconnect
  reconnect -> none:
    if on_reconnect: on_reconnect.call
    wrapped_.reconnect

  is_closed -> bool: return wrapped_.is_closed

/**
Tests that the client continues to reconnect if the transport reconnect fails.
*/
test create_transport/Lambda --logger/log.Logger:
  failing_transport /TestTransport? := null

  create_failing_transport := ::
    transport := create_transport.call
    failing_transport = TestTransport transport
    failing_transport

  reconnection_strategy := mqtt.DefaultSessionReconnectionStrategy
      --logger=logger.with_name "mqtt.reconnection_strategy"
      --attempt_delays=[
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
      ]

  with_packet_client create_failing_transport
      --client_id = "disconnect-client1"
      --no-clean_session
      --reconnection_strategy = reconnection_strategy
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    is_destroyed := false

    reconnect_attempt := 0
    reconnect_was_attempted := Latch
    failing_transport.on_reconnect = ::
      reconnect_attempt++
      if reconnect_attempt == 0:
        null
      else if reconnect_attempt <= 2:
        throw "RECONNECTION FAILING"
      // Finally it connects again.
      is_destroyed = false
      null

    failing_transport.on_write = ::
      if is_destroyed: throw "destroyed transport"

    wait_for_idle.call
    clear.call

    // Destroy the transport. From the client's side it looks as if all writes fail from now on.
    is_destroyed = true

    write_failed_latch := Latch

    // This packet will make it through after several reconnection attempts.
    client.publish "failing" #[] --qos=0

    expect reconnect_attempt > 2
