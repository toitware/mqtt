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
Tests that the client closes as if it was a forced close if the connection is down.
*/
test_no_disconnect_packet create_transport/Lambda --logger/log.Logger:
  failing_transport /TestTransport? := null

  create_failing_transport := ::
    transport := create_transport.call
    failing_transport = TestTransport transport
    failing_transport

  // There will be a reconnection attempt immediately when the connection fails.
  // The second and third attempts are delayed as follows:
  second_attempt_delay := Duration --ms=1
  third_attempt_delay := Duration --s=10
  reconnection_strategy := mqtt.DefaultSessionReconnectionStrategy
      --logger=logger.with_name "mqtt.reconnection_strategy"
      --attempt_delays=[
        second_attempt_delay,
        third_attempt_delay,
      ]
  with_packet_client create_failing_transport
      --client_id = "disconnect-client1"
      --no-clean_session
      --reconnection_strategy = reconnection_strategy
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    reconnect_was_attempted := monitor.Latch
    failing_transport.on_reconnect = ::
      if not reconnect_was_attempted.has_value: reconnect_was_attempted.set true

    is_destroyed := false
    failing_transport.on_write = ::
      if is_destroyed: throw "destroyed transport"

    wait_for_idle.call
    clear.call

    // Destroy the transport. From the client's side it looks as if all writes fail from now on.
    is_destroyed = true

    write_failed_latch := monitor.Latch
    task::
      exception := catch:
        // This packet will never make it through, as the transport is failing.
        client.publish "failing" #[] --qos=0
      expect_not_null exception
      write_failed_latch.set exception

    reconnect_was_attempted.get

    start_time := Time.now
    // The close call should behave as if it was a force close, as the connection is currently not alive.
    // Give the reconnection strategy time to try to wait for the second attempt.
    // (We want to test the strategy doesn't sleep 10 seconds, when we call 'close').
    sleep --ms=30
    // At this point the reconnection strategy has tried once, and should be sleeping for 10 seconds
    // for the second attempt.
    client.close
    close_duration := Duration.since start_time
    expect close_duration < third_attempt_delay

    expect client.is_closed

    write_failed_latch.get

    activity := get_activity.call
    // We never connected again.
    expect (activity.filter: it[0] == "write" and it[1] is mqtt.ConnectPacket).is_empty
    // There should be at most 2 reconnect attempts.
    // One without delay, when trying to reconnect.
    // Then another after the first delay (1ms) has elapsed.
    // The third attempt should only happen after 10s, and the program should have finished
    // at this point.
    expect (activity.filter: it[0] == "reconnect").size <= 2


/**
Tests that the client waits for the reconnect attempt to finish before it closes.

This is different from $test_no_disconnect_packet, as the client already managed to
  establish a connecting in this test scenario. Since the connection already exists, the client
  sends a disconnect instead of abruptly closing the connection.
*/
test_reconnect_before_disconnect_packet create_transport/Lambda --logger/log.Logger:
  brittle_transport /TestTransport? := null

  create_brittle_transport := ::
    transport := create_transport.call
    brittle_transport = TestTransport transport
    brittle_transport

  reconnection_strategy := mqtt.DefaultSessionReconnectionStrategy
      --logger=logger.with_name "mqtt.reconnection_strategy"
      --attempt_delays=[ Duration.ZERO ]
  with_packet_client create_brittle_transport
      --client_id = "disconnect-client1"
      --no-clean_session
      --reconnection_strategy = reconnection_strategy
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    reconnect_was_attempted := monitor.Latch
    is_destroyed := false
    delay_write_latch := monitor.Latch
    write_after_reconnect := 0

    brittle_transport.on_reconnect = ::
      if not reconnect_was_attempted.has_value: reconnect_was_attempted.set true
      is_destroyed = false
      write_after_reconnect = 0

    brittle_transport.on_write = ::
      if is_destroyed: throw "destroyed transport"
      if reconnect_was_attempted.has_value and write_after_reconnect++ == 1:
        delay_write_latch.get

    wait_for_idle.call
    clear.call

    // Temporarily destroy the transport. From the client's side it looks as if all writes fail from now on.
    is_destroyed = true

    write_succeeded_latch := monitor.Latch
    task::
      exception := catch:
        // This packet will succeed. First, it fails because the transport is broken, but then
        // the transport reconnects and the packet will be sent.
        client.publish "succeeding" #[] --qos=0
      write_succeeded_latch.set exception

    reconnect_was_attempted.get
    delay_write_latch.set "unblock writing of connect packet"
    // The 'close' function will disable reconnection attempts, but the current one will succeed.
    // Once the packet was written, we will send the disconnect packet.
    client.close

    expect client.is_closed

    expect_null write_succeeded_latch.get

    activity := get_activity.call

    // We managed to connect again.
    expect_equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.ConnectPacket).size
    // Since we connected, we also sent a disconnect packet.
    expect_equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.DisconnectPacket).size

close_in_handle create_transport/Lambda --logger/log.Logger --force/bool:
  client := mqtt.FullClient --transport=create_transport.call --logger=logger

  options := mqtt.SessionOptions --client_id="close_in_handle"
  client.connect --options=options

  handle_done := monitor.Latch
  task::
    client.handle: | packet |
      if packet is mqtt.PublishPacket:
        publish := packet as mqtt.PublishPacket
        if publish.topic == "disconnect":
          client.close --force=force
    handle_done.set true

  client.when_running:
    client.subscribe "disconnect"
    client.publish "disconnect" #[] --qos=0

  handle_done.get

test_reconnect_after_broker_disconnect create_transport/Lambda --logger/log.Logger:
  disconnecting_transport /TestTransport? := null

  create_failing_transport := ::
    transport := create_transport.call
    disconnecting_transport = TestTransport transport
    disconnecting_transport

  with_packet_client create_failing_transport
      --client_id = "disconnect-client1"
      --no-clean_session
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    reconnect_was_attempted := monitor.Latch
    is_disconnected := false

    disconnecting_transport.on_reconnect = ::
      is_disconnected = false
      if not reconnect_was_attempted.has_value: reconnect_was_attempted.set true

    disconnecting_transport.on_read = :: | wrapped |
      is_disconnected ? null : wrapped.read

    wait_for_idle.call
    clear.call

    // Disconnect. From the client's side it looks as if the broker disconnected.
    is_disconnected = true

    // The test-transport already started reading from the wrapped transport, so just
    // changing the boolean doesn't yet have any effect. We need to receive a packet first.
    // After that, the client will try to read again and see the disconnect.

    // We are sending a packet with QOS=1.
    // This will lead to a QoS response from the broker, after which the 'is_disconnected' will trigger.
    client.publish "trigger a packet" #[] --qos=1

    // At this point we hope to see a reconnection attempt.
    reconnect_was_attempted.get

/**
Tests the client's close function.
*/
test create_transport/Lambda --logger/log.Logger:
  test_no_disconnect_packet create_transport --logger=logger
  // TODO(floitsch): reenable this test.
  // test_reconnect_before_disconnect_packet create_transport --logger=logger
  close_in_handle create_transport --logger=logger --no-force
  close_in_handle create_transport --logger=logger --force
  test_reconnect_after_broker_disconnect create_transport --logger=logger

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  if test_with_mosquitto: return

  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
