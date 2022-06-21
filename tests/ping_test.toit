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
import .transport
import .packet_test_client

/**
Tests that the client and broker correctly ack packets.
*/
test create_transport/Lambda logger/log.Logger:
  with_packet_client create_transport
      --keep_alive = (Duration --s=1)
      --logger=logger : | client/mqtt.FullClient _ _ get_activity/Lambda |
    sleep --ms=2_000

    activity := get_activity.call
    sent_ping := activity.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket
    expect sent_ping

/**
Tests that the client and broker work with 0-duration keep-alive.

According to the spec a 0 keep-alive, just means that there isn't any.
*/
test_no_timeout create_transport/Lambda logger/log.Logger:
  with_packet_client create_transport
      --keep_alive = Duration.ZERO
      --logger=logger : | client/mqtt.FullClient _ _ get_activity/Lambda |
    sleep --ms=2_000

    activity := get_activity.call
    sent_ping := activity.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket
    expect_not sent_ping

/**
A transport that can be slowed down for writing.
*/
class SlowTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport
  should_write_slowly /bool := false

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    if should_write_slowly:
      sleep --ms=200
      wrapped_.write bytes[0..1]
      return 1
    else:
      return wrapped_.write bytes

  read -> ByteArray?: return wrapped_.read
  close -> none: wrapped_.close
  supports_reconnect -> bool: return wrapped_.supports_reconnect
  reconnect -> none: wrapped_.reconnect
  is_closed -> bool: return wrapped_.is_closed

/**
Tests that the client doesn't send ping requests if the write is just slow.

If it takes a while to send a packet, but we are seeing progress, then there is
  no need to send a request.

Note: it is safe to change this test with a better activity manager that also
  takes data from the broker into account.
*/
test_slow_write create_transport/Lambda logger/log.Logger:
  slow_transport /SlowTransport? := null

  create_slow_transport := ::
    transport := create_transport.call
    slow_transport = SlowTransport transport
    slow_transport

  keep_alive := Duration --s=1
  with_packet_client create_slow_transport
      --keep_alive = keep_alive
      --logger=logger : | client/mqtt.FullClient wait_for_idle/Lambda _ get_activity/Lambda |

    start_time := Time.now
    slow_transport.should_write_slowly = true
    client.publish "test" "test".to_byte_array
    slow_transport.should_write_slowly = false
    end_time := Time.now

    wait_for_idle.call

    // Despite the writing taking longer than the keep_alive, the client doesn't send a
    // ping request as it can see that the packet is being written and makes slow progress.
    expect (start_time.to end_time) > keep_alive
    activity := get_activity.call
    sent_ping := activity.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket

    // This behavior is safe to change with a different activity manager.
    // When changing things here, update the comment of this function.
    expect_not sent_ping

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  // Run the tests in parallel, as they are mostly sleeping.

  if test_with_mosquitto:
    task:: with_mosquitto --logger=logger: test it logger
    // Older mosquitto brokers have a bug that they don't see incoming bytes
    // as activity.
    if not get_mosquitto_version.starts_with "1.":
      task:: with_mosquitto --logger=logger: test_slow_write it logger

  else:
    task:: with_internal_broker --logger=logger: test it logger
    task:: with_internal_broker --logger=logger: test_slow_write it logger
    // We can't test the no-timeout test with mosquitto, as it doesn't support 0 keep-alive.
    task:: with_internal_broker --logger=logger: test_no_timeout it logger
