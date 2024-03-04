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

class TestTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport

  on-reconnect /Lambda? := null
  on-disconnect /Lambda? := null
  on-write /Lambda? := null
  on-read /Lambda? := null

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    if on-write: on-write.call bytes
    return wrapped_.write bytes

  read -> ByteArray?:
    if on-read: return on-read.call wrapped_
    return wrapped_.read

  close -> none: wrapped_.close

  supports-reconnect -> bool: return wrapped_.supports-reconnect

  reconnect -> none:
    if on-reconnect: on-reconnect.call
    wrapped_.reconnect

  disconnect -> none:
    if on-disconnect: on-disconnect.call
    wrapped_.disconnect

  is-closed -> bool: return wrapped_.is-closed

/**
Tests that the client blocks when too many messages are in-flight.
*/
test create-transport/Lambda --logger/log.Logger:
  MAX-INFLIGHT ::= 3
  test-transport /TestTransport? := null

  create-test-transport := ::
    transport := create-transport.call
    test-transport = TestTransport transport
    test-transport

  // There will be a reconnection attempt immediately when the connection fails.
  // The second and third attempts are delayed as follows:
  second-attempt-delay := Duration --ms=1
  third-attempt-delay := Duration --s=10
  with-packet-client create-test-transport
      --client-id = "inflight-client1"
      --max-inflight = MAX-INFLIGHT
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    should-withhold := false
    withhold-signal := monitor.Signal
    test-transport.on-read = ::
      message := it.read
      withhold-signal.wait: not should-withhold
      message

    wait-for-idle.call
    clear.call

    // Start to withhold messages from the broker.
    should-withhold = true

    sent-count := 0
    write-failed-latch := monitor.Latch
    task::
      (MAX-INFLIGHT + 1).repeat: | i |
        // Try to send more than the max-inflight messages.
        // The transport is blocking all reads. This means that the client will
        //   not be able to receive any PUBACKs, and eventually block.
        exception := catch:
          with-timeout --ms=20:
            client.publish "some_topic" #[] --qos=1
            sent-count++
        if i < MAX-INFLIGHT:
          expect-null exception
        else:
          expect-not-null exception
          write-failed-latch.set exception

    write-failed-latch.get
    // The client failed to write a message, as there were too many messages in-flight.
    // Release the messages in the test transport.
    should-withhold = false
    withhold-signal.raise

    wait-for-idle.call

    activity := get-activity.call

    // We should have gotten all the acks now.
    expect-equals MAX-INFLIGHT (activity.filter: it[0] == "read" and it[1] is mqtt.PubAckPacket).size

    clear.call

    // Should not block anymore.
    client.publish "some_topic" #[] --qos=1

    wait-for-idle.call

    activity = get-activity.call
    expect-equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).size
    expect-equals 1 (activity.filter: it[0] == "read" and it[1] is mqtt.PubAckPacket).size

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  if test-with-mosquitto: return

  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  with-internal-broker --logger=logger run-test
