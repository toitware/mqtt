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

class TestTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport

  on_reconnect /Lambda? := null
  on_disconnect /Lambda? := null
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

  disconnect -> none:
    if on_disconnect: on_disconnect.call
    wrapped_.disconnect

  is_closed -> bool: return wrapped_.is_closed

/**
Tests that the client blocks when too many messages are in-flight.
*/
test create_transport/Lambda --logger/log.Logger:
  MAX_INFLIGHT ::= 3
  test_transport /TestTransport? := null

  create_test_transport := ::
    transport := create_transport.call
    test_transport = TestTransport transport
    test_transport

  // There will be a reconnection attempt immediately when the connection fails.
  // The second and third attempts are delayed as follows:
  second_attempt_delay := Duration --ms=1
  third_attempt_delay := Duration --s=10
  with_packet_client create_test_transport
      --client_id = "inflight-client1"
      --max_inflight = MAX_INFLIGHT
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    should_withhold := false
    withhold_signal := monitor.Signal
    test_transport.on_read = ::
      message := it.read
      withhold_signal.wait: not should_withhold
      message

    wait_for_idle.call
    clear.call

    // Start to withhold messages from the broker.
    should_withhold = true

    sent_count := 0
    write_failed_latch := monitor.Latch
    task::
      (MAX_INFLIGHT + 1).repeat: | i |
        // Try to send more than the max-inflight messages.
        // The transport is blocking all reads. This means that the client will
        //   not be able to receive any PUBACKs, and eventually block.
        exception := catch:
          with_timeout --ms=20:
            client.publish "some_topic" #[] --qos=1
            sent_count++
        if i < MAX_INFLIGHT:
          expect_null exception
        else:
          expect_not_null exception
          write_failed_latch.set exception

    write_failed_latch.get
    // The client failed to write a message, as there were too many messages in-flight.
    // Release the messages in the test transport.
    should_withhold = false
    withhold_signal.raise

    wait_for_idle.call

    activity := get_activity.call

    // We should have gotten all the acks now.
    expect_equals MAX_INFLIGHT (activity.filter: it[0] == "read" and it[1] is mqtt.PubAckPacket).size

    clear.call

    // Should not block anymore.
    client.publish "some_topic" #[] --qos=1

    wait_for_idle.call

    activity = get_activity.call
    expect_equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).size
    expect_equals 1 (activity.filter: it[0] == "read" and it[1] is mqtt.PubAckPacket).size

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  if test_with_mosquitto: return

  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
