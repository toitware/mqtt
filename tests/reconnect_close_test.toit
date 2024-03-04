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
Tests that the client closes as if it was a forced close if the connection is down.
*/
test-no-disconnect-packet create-transport/Lambda --logger/log.Logger:
  failing-transport /CallbackTestTransport? := null

  create-failing-transport := ::
    transport := create-transport.call
    failing-transport = CallbackTestTransport transport
    failing-transport

  // There will be a reconnection attempt immediately when the connection fails.
  // The second and third attempts are delayed as follows:
  second-attempt-delay := Duration --ms=1
  third-attempt-delay := Duration --s=10
  reconnection-strategy := mqtt.RetryReconnectionStrategy
      --logger=logger.with-name "mqtt.reconnection_strategy"
      --attempt-delays=[
        second-attempt-delay,
        third-attempt-delay,
      ]
  with-packet-client create-failing-transport
      --client-id = "disconnect-client1"
      --no-clean-session
      --reconnection-strategy = reconnection-strategy
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    reconnect-was-attempted := monitor.Latch
    failing-transport.on-reconnect = ::
      if not reconnect-was-attempted.has-value: reconnect-was-attempted.set true

    is-destroyed := false
    failing-transport.on-write = ::
      if is-destroyed: throw "destroyed transport"

    wait-for-idle.call
    clear.call

    // Destroy the transport. From the client's side it looks as if all writes fail from now on.
    is-destroyed = true

    write-failed-latch := monitor.Latch
    task::
      exception := catch:
        // This packet will never make it through, as the transport is failing.
        client.publish "failing" #[] --qos=0
      expect-not-null exception
      write-failed-latch.set exception

    reconnect-was-attempted.get

    start-time := Time.now
    // The close call should behave as if it was a force close, as the connection is currently not alive.
    // Give the reconnection strategy time to try to wait for the second attempt.
    // (We want to test the strategy doesn't sleep 10 seconds, when we call 'close').
    sleep --ms=30
    // At this point the reconnection strategy has tried once, and should be sleeping for 10 seconds
    // for the second attempt.
    client.close
    close-duration := Duration.since start-time
    expect close-duration < third-attempt-delay

    expect client.is-closed

    write-failed-latch.get

    activity := get-activity.call
    // We never connected again.
    expect (activity.filter: it[0] == "write" and it[1] is mqtt.ConnectPacket).is-empty
    // There should be at most 2 reconnect attempts.
    // One without delay, when trying to reconnect.
    // Then another after the first delay (1ms) has elapsed.
    // The third attempt should only happen after 10s, and the program should have finished
    // at this point.
    expect (activity.filter: it[0] == "reconnect").size <= 2


/**
Tests that the client waits for the reconnect attempt to finish before it closes.

This is different from $test-no-disconnect-packet, as the client already managed to
  establish a connecting in this test scenario. Since the connection already exists, the client
  sends a disconnect instead of abruptly closing the connection.
*/
test-reconnect-before-disconnect-packet create-transport/Lambda --logger/log.Logger:
  brittle-transport /CallbackTestTransport? := null

  create-brittle-transport := ::
    transport := create-transport.call
    brittle-transport = CallbackTestTransport transport
    brittle-transport

  reconnection-strategy := mqtt.RetryReconnectionStrategy
      --logger=logger.with-name "mqtt.reconnection_strategy"
      --attempt-delays=[ Duration.ZERO ]
  with-packet-client create-brittle-transport
      --client-id = "disconnect-client1"
      --no-clean-session
      --reconnection-strategy = reconnection-strategy
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    reconnect-was-attempted := monitor.Latch
    is-destroyed := false
    delay-write-latch := monitor.Latch
    write-after-reconnect := 0

    brittle-transport.on-reconnect = ::
      if not reconnect-was-attempted.has-value: reconnect-was-attempted.set true
      is-destroyed = false
      write-after-reconnect = 0

    brittle-transport.on-write = ::
      if is-destroyed: throw "destroyed transport"
      if reconnect-was-attempted.has-value and write-after-reconnect++ == 1:
        delay-write-latch.get

    wait-for-idle.call
    clear.call

    // Temporarily destroy the transport. From the client's side it looks as if all writes fail from now on.
    is-destroyed = true

    write-succeeded-latch := monitor.Latch
    task::
      exception := catch:
        // This packet will succeed. First, it fails because the transport is broken, but then
        // the transport reconnects and the packet will be sent.
        client.publish "succeeding" #[] --qos=0
      write-succeeded-latch.set exception

    reconnect-was-attempted.get
    delay-write-latch.set "unblock writing of connect packet"
    // The 'close' function will disable reconnection attempts, but the current one will succeed.
    // Once the packet was written, we will send the disconnect packet.
    client.close

    expect client.is-closed

    expect-null write-succeeded-latch.get

    activity := get-activity.call

    // We managed to connect again.
    expect-equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.ConnectPacket).size
    // Since we connected, we also sent a disconnect packet.
    expect-equals 1 (activity.filter: it[0] == "write" and it[1] is mqtt.DisconnectPacket).size

close-in-handle create-transport/Lambda --logger/log.Logger --force/bool:
  client := mqtt.FullClient --transport=create-transport.call --logger=logger

  options := mqtt.SessionOptions --client-id="close_in_handle"
  client.connect --options=options

  handle-done := monitor.Latch
  task::
    client.handle: | packet |
      if packet is mqtt.PublishPacket:
        publish := packet as mqtt.PublishPacket
        if publish.topic == "disconnect":
          client.close --force=force
    handle-done.set true

  client.when-running:
    client.subscribe "disconnect"
    client.publish "disconnect" #[] --qos=0

  handle-done.get

test-reconnect-after-broker-disconnect create-transport/Lambda --logger/log.Logger:
  disconnecting-transport /CallbackTestTransport? := null

  create-failing-transport := ::
    transport := create-transport.call
    disconnecting-transport = CallbackTestTransport transport
    disconnecting-transport

  with-packet-client create-failing-transport
      --client-id = "disconnect-client1"
      --no-clean-session
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    reconnect-was-attempted := monitor.Latch
    is-disconnected := false

    disconnecting-transport.on-reconnect = ::
      is-disconnected = false
      if not reconnect-was-attempted.has-value: reconnect-was-attempted.set true

    disconnecting-transport.on-read = :: | wrapped |
      is-disconnected ? null : wrapped.read

    wait-for-idle.call
    clear.call

    // Disconnect. From the client's side it looks as if the broker disconnected.
    is-disconnected = true

    // The test-transport already started reading from the wrapped transport, so just
    // changing the boolean doesn't yet have any effect. We need to receive a packet first.
    // After that, the client will try to read again and see the disconnect.

    // We are sending a packet with QOS=1.
    // This will lead to a QoS response from the broker, after which the 'is_disconnected' will trigger.
    client.publish "trigger a packet" #[] --qos=1

    // At this point we hope to see a reconnection attempt.
    reconnect-was-attempted.get

/**
Tests the client's close function.
*/
test create-transport/Lambda --logger/log.Logger:
  test-no-disconnect-packet create-transport --logger=logger
  // TODO(floitsch): reenable this test.
  // test_reconnect_before_disconnect_packet create_transport --logger=logger
  close-in-handle create-transport --logger=logger --no-force
  close-in-handle create-transport --logger=logger --force
  test-reconnect-after-broker-disconnect create-transport --logger=logger

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  if test-with-mosquitto: return

  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  with-internal-broker --logger=logger run-test
