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
Tests that the clean session flag is correctly handled.

When the clean session flag is given, then the session should start fresh. Also, the broker
  should discard any state when the client disconnects (gracefully or not).

When the clean session flag is not given, then the old session should be found (assuming
  there is one).
*/
test-clean-session create-transport/Lambda --logger/log.Logger:
  // First connection should be clean.
  with-packet-client create-transport
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |
    activity := get-activity.call
    connect-ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect-not connect-ack.session-present

  // Second connection should be clean too.
  with-packet-client create-transport
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |
    activity := get-activity.call
    connect-ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect-not connect-ack.session-present

  // Third connection should be clean, but we now connect without clean_session.
  with-packet-client create-transport
      --no-clean-session
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |
    activity := get-activity.call
    connect-ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect-not connect-ack.session-present

  // Now we should have a session.
  with-packet-client create-transport
      --no-clean-session
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |
    activity := get-activity.call
    connect-ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect connect-ack.session-present

  // Connecting again with a clean-session flag yields in no-session again.
  // Third connection should be clean, but we now connect without clean_session.
  with-packet-client create-transport
      --logger=logger : | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |
    activity := get-activity.call
    connect-ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect-not connect-ack.session-present

/**
Tests that the subscriptions are remembered when the broker has a session for the client.

This is the easiest way to see whether the broker keeps some state for the client.

Also test that messages to subscription are buffered when the client isn't connected.
*/
test-subscriptions create-transport/Lambda --logger/log.Logger:
  topic := "session-topic"

  // Clear the sub-client session.
  with-packet-client create-transport
      --client-id = "sub-client"
      --clean-session
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |
    null

  with-packet-client create-transport
      --client-id = "sub-client"
      --no-clean-session
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    // Subscribe to topic, and then immediately close the connection.
    client.subscribe topic
    wait-for-idle.call

  with-packet-client create-transport
      --logger = logger
      --client-id = "other-client": | other-client/mqtt.FullClient _ _ _ |

    // This message is sent when the client is not alive.
    // The broker's session will cache it and send it when the client reconnects.
    other-client.publish topic "hello0".to-byte-array --qos=1

    with-packet-client create-transport
        --client-id = "sub-client"
        --no-clean-session
        --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

      wait-for-idle.call
      activity := get-activity.call
      messages := activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
      message := messages.first[1]
      expect-equals topic message.topic
      expect-equals "hello0" message.payload.to-string

      clear.call

      other-client.publish topic "hello1".to-byte-array --qos=0

      saw-message := false
      for i := 0; i < 5; i++:
        activity = get-activity.call
        messages = activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
        if messages.is-empty:
          sleep --ms=(100 * i)
          continue
        message = messages.first[1]
        expect-equals topic message.topic
        expect-equals "hello1" message.payload.to-string
        saw-message = true
        break
      expect saw-message

/**
Tests that the client resends messages when it hasn't received the broker's acks.
*/
test-client-qos create-transport/Lambda --logger/log.Logger:
  client-id := "test-client-qos"
  topic := "test-resend"

  // Connect once to clear the session.
  with-packet-client create-transport
      --logger = logger
      --client-id = client-id
      --clean-session:
    null

  // The read filter intercepts the first puback and sets the latch.
  intercepted-latch := monitor.Latch
  read-filter := :: | packet/mqtt.Packet |
    if not intercepted-latch.has-value and packet is mqtt.PubAckPacket:
      intercepted-latch.set (packet as mqtt.PubAckPacket).packet-id
      null
    else:
      packet

  first-after-intercepted-pub-ack := true
  write-filter := :: | packet/mqtt.Packet |
    if intercepted-latch.has-value and first-after-intercepted-pub-ack:
      first-after-intercepted-pub-ack = false
      throw "disconnect"
    packet

  with-packet-client create-transport
      --client-id = client-id
      --read-filter = read-filter
      --write-filter = write-filter
      --no-clean-session
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    wait-for-idle.call
    clear.call

    client.publish topic "ack is lost".to-byte-array --qos=1
    intercepted-packet-id := intercepted-latch.get
    clear.call

    // The ack from the server was intercepted.
    // Bring down the transport to simulate a disconnect.
    // The client should reconnect and resend the message.
    client.publish "random_topic" "write_filter_intercepts".to-byte-array --qos=0

    wait-for-idle.call

    activity := get-activity.call
    expect (activity.any: it[0] == "reconnect")
    publish-packets := (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).map: it[1]
    // We expect two packets:
    // - the resent packet
    // - the packet for which we got an exception
    expect-equals 2 publish-packets.size
    resent /mqtt.PublishPacket := publish-packets[0]
    random-topic-packet /mqtt.PublishPacket := publish-packets[1]
    expect-equals topic resent.topic
    expect resent.duplicate
    expect-equals intercepted-packet-id resent.packet-id

    ack-packets := (activity.filter: it[0] == "read" and it[1] is mqtt.PubAckPacket).map: it[1]
    expect-equals 1 ack-packets.size

/**
Tests that the broker resends messages when it hasn't received the client's acks.
*/
test-broker-qos create-transport/Lambda --logger/log.Logger:
  // Create a client that will send a message to the client we are testing.
  with-packet-client create-transport
      --clean-session
      --logger=logger: | other-client/mqtt.FullClient _ _ _ |

    client-id := "test-broker-qos"
    topic := "test-resend"

    // Connect once to clear the session.
    with-packet-client create-transport
        --logger = logger
        --client-id = client-id
        --clean-session:
      null

    // The write filter intercepts the first puback and sets the latch.
    intercepted-latch := monitor.Latch

    ack-counter := 0
    write-filter := :: | packet/mqtt.Packet |
      if packet is mqtt.PubAckPacket: ack-counter++
      if ack-counter == 3 and not intercepted-latch.has-value and packet is mqtt.PubAckPacket:
        intercepted-latch.set (packet as mqtt.PubAckPacket).packet-id
        throw "disconnect"
      else:
        packet

    with-packet-client create-transport
        --client-id = client-id
        --write-filter = write-filter
        --no-clean-session
        --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

      client.subscribe topic
      wait-for-idle.call
      clear.call

      other-client.publish topic "increase packet id".to-byte-array --qos=1
      other-client.publish topic "increase packet id".to-byte-array --qos=1

      wait-for-idle.call
      clear.call

      // The other client now sends a message for which the ack is lost,
      // and where the server needs to resend the message.
      other-client.publish topic "ack is lost".to-byte-array --qos=1
      intercepted-packet-id := intercepted-latch.get

      wait-for-idle.call

      activity := get-activity.call
      expect (activity.any: it[0] == "reconnect")

      publish-packets := (activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket).map: it[1]
      // We expect two packets:
      // - the initial packet
      // - the resent packet
      expect-equals 2 publish-packets.size
      original /mqtt.PublishPacket := publish-packets[0]
      resent /mqtt.PublishPacket := publish-packets[1]
      expect-equals topic original.topic
      expect-equals topic resent.topic
      expect-equals original.packet-id resent.packet-id
      expect-not original.duplicate
      expect resent.duplicate
      expect-equals intercepted-packet-id resent.packet-id

      ack-packets := (activity.filter: it[0] == "write" and it[1] is mqtt.PubAckPacket).map: it[1]
      // We immediately resend the ack packet as soon as the connection is established again.
      // However, the broker also sends the same packet again.
      // We end up with two acks for the same packet.
      expect-equals 2 ack-packets.size
      2.repeat:
        expect-equals intercepted-packet-id ack-packets[it].packet-id

test create-transport/Lambda --logger/log.Logger:
  test-clean-session create-transport --logger=logger
  test-subscriptions create-transport --logger=logger
  test-client-qos create-transport --logger=logger
  test-broker-qos create-transport --logger=logger

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
