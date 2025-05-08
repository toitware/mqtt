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
import system

import .broker-internal
import .broker-mosquitto
import .transport
import .packet-test-client

/**
Tests that the client and broker correctly ack packets.
*/
test create-transport/Lambda logger/log.Logger:
  with-packet-client create-transport
      --keep-alive = (Duration --s=1)
      --logger=logger : | client/mqtt.FullClient _ _ get-activity/Lambda |
    sleep --ms=2_000

    activity := get-activity.call
    sent-ping := activity.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket
    expect sent-ping

/**
Tests that the client and broker work with 0-duration keep-alive.

According to the spec a 0 keep-alive, just means that there isn't any.
*/
test-no-timeout create-transport/Lambda logger/log.Logger:
  with-packet-client create-transport
      --keep-alive = Duration.ZERO
      --logger=logger : | client/mqtt.FullClient _ _ get-activity/Lambda |
    sleep --ms=2_000

    activity := get-activity.call
    sent-ping := activity.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket
    expect-not sent-ping

/**
A transport that can be slowed down for writing.
*/
class SlowTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport
  should-write-slowly /bool := false

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    if should-write-slowly:
      sleep --ms=200
      wrapped_.write bytes[0..1]
      return 1
    else:
      return wrapped_.write bytes

  read -> ByteArray?: return wrapped_.read
  close -> none: wrapped_.close
  supports-reconnect -> bool: return wrapped_.supports-reconnect
  reconnect -> none: wrapped_.reconnect
  disconnect -> none: wrapped_.disconnect
  is-closed -> bool: return wrapped_.is-closed

/**
Tests that the client doesn't send ping requests if the write is just slow.

If it takes a while to send a packet, but we are seeing progress, then there is
  no need to send a request.

Note: it is safe to change this test with a better activity manager that also
  takes data from the broker into account.
*/
test-slow-write create-transport/Lambda logger/log.Logger:
  slow-transport /SlowTransport? := null

  create-slow-transport := ::
    transport := create-transport.call
    slow-transport = SlowTransport transport
    slow-transport

  keep-alive := Duration --s=1
  with-packet-client create-slow-transport
      --keep-alive = keep-alive
      --logger=logger : | client/mqtt.FullClient wait-for-idle/Lambda _ get-activity/Lambda |

    start-time := Time.now
    slow-transport.should-write-slowly = true
    client.publish "test" "test".to-byte-array
    slow-transport.should-write-slowly = false
    end-time := Time.now

    wait-for-idle.call

    // Despite the writing taking longer than the keep_alive, the client doesn't send a
    // ping request as it can see that the packet is being written and makes slow progress.
    expect (start-time.to end-time) > keep-alive
    activity := get-activity.call
    sent-ping := activity.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket

    // This behavior is safe to change with a different activity manager.
    // When changing things here, update the comment of this function.
    expect-not sent-ping

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  // Run the tests in parallel, as they are mostly sleeping.

  if test-with-mosquitto:
    task:: with-mosquitto --logger=logger: test it logger
    // Older mosquitto brokers have a bug that they don't see incoming bytes
    // as activity. Same seems to be the case for macOS.
    if not (get-mosquitto-version.starts-with "1." or system.platform == system.PLATFORM-MACOS):
      task:: with-mosquitto --logger=logger: test-slow-write it logger

  else:
    task:: with-internal-broker --logger=logger: test it logger
    task:: with-internal-broker --logger=logger: test-slow-write it logger
    // We can't test the no-timeout test with mosquitto, as it doesn't support 0 keep-alive.
    task:: with-internal-broker --logger=logger: test-no-timeout it logger
