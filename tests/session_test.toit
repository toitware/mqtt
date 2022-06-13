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

/**
Tests that the clean session flag is correctly handled.

When the clean session flag is given, then the session should start fresh. Also, the broker
  should discard any state when the client disconnects (gracefully or not).

When the clean session flag is not given, then the old session should be found (assuming there
  is one).
*/
test_clean_session create_transport/Lambda --logger/log.Logger:
  // First connection should be clean.
  with_packet_client create_transport
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    activity := get_activity.call
    connect_ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

  // Second connection should be clean too.
  with_packet_client create_transport
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    activity := get_activity.call
    connect_ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

  // Third connection should be clean, but we now connect without clean_session.
  with_packet_client create_transport
      --no-clean_session
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    activity := get_activity.call
    connect_ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

  // Now we should have a session.
  with_packet_client create_transport
      --no-clean_session
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    activity := get_activity.call
    connect_ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect connect_ack.session_present

  // Connecting again with a clean-session flag yields in no-session again.
  // Third connection should be clean, but we now connect without clean_session.
  with_packet_client create_transport
      --logger=logger : | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    activity := get_activity.call
    connect_ack /mqtt.ConnAckPacket := (activity.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

/**
Tests that the subscriptions are remembered when the broker has a session for the client.

This is the easiest way to see whether the broker keeps some state for the client.

Also test that messages to subscription are buffered when the client isn't connected.
*/
test_subscriptions create_transport/Lambda --logger/log.Logger:
  topic := "session-topic"

  // Clear the sub-client session.
  with_packet_client create_transport
      --client_id = "sub-client"
      --clean_session
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    null

  with_packet_client create_transport
      --client_id = "sub-client"
      --no-clean_session
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    // Subscribe to topic, and then immediately close the connection.
    client.subscribe topic
    wait_for_idle.call

  with_packet_client create_transport
      --logger = logger
      --client_id = "other-client": | other_client/mqtt.FullClient _ _ _ |

    // This message is sent when the client is not alive.
    // The broker's session will cache it and send it when the client reconnects.
    other_client.publish topic "hello0".to_byte_array --qos=1

    with_packet_client create_transport
        --client_id = "sub-client"
        --no-clean_session
        --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

      wait_for_idle.call
      activity := get_activity.call
      messages := activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
      message := messages.first[1]
      expect_equals topic message.topic
      expect_equals "hello0" message.payload.to_string

      clear.call

      other_client.publish topic "hello1".to_byte_array --qos=0

      saw_message := false
      for i := 0; i < 5; i++:
        activity = get_activity.call
        messages = activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
        if messages.is_empty:
          sleep --ms=(100 * i)
          continue
        message = messages.first[1]
        expect_equals topic message.topic
        expect_equals "hello1" message.payload.to_string
        saw_message = true
        break
      expect saw_message

/**
Tests that the client resends messages when it hasn't received the broker's acks.
*/
test_client_qos create_transport/Lambda --logger/log.Logger:
  client_id := "test-client-qos"
  topic := "test-resend"

  // Connect once to clear the session.
  with_packet_client create_transport
      --logger = logger
      --client_id = client_id
      --clean_session:
    null

  created_transport /mqtt.Transport? := null

  // Intercept the transport creation, so we can close the transport from the outside.
  create_transport_intercept := ::
    created_transport = create_transport.call
    created_transport

  // The read filter intercepts the first puback and sets the latch.
  intercepted_latch := Latch
  read_filter := :: | packet/mqtt.Packet |
    if not intercepted_latch.has_value and packet is mqtt.PubAckPacket:
      intercepted_latch.set (packet as mqtt.PubAckPacket).packet_id
      null
    else:
      packet

  first_after_intercepted_pub_ack := true
  write_filter := :: | packet/mqtt.Packet |
    if intercepted_latch.has_value and first_after_intercepted_pub_ack:
      first_after_intercepted_pub_ack = false
      throw "disconnect"
    packet

  with_packet_client create_transport
      --client_id = client_id
      --read_filter = read_filter
      --write_filter = write_filter
      --no-clean_session
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
    assert: created_transport != null

    wait_for_idle.call
    clear.call

    client.publish topic "ack is lost".to_byte_array --qos=1
    intercepted_packet_id := intercepted_latch.get
    clear.call

    // The ack from the server was intercepted.
    // Bring down the transport to simulate a disconnect.
    // The client should reconnect and resend the message.
    client.publish "random_topic" "write_filter_intercepts".to_byte_array --qos=0

    wait_for_idle.call

    activity := get_activity.call
    expect (activity.any: it[0] == "reconnect")
    publish_packets := (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).map: it[1]
    // We expect three packets:
    // - the resent packet
    // - the packet for which we got an exception
    // - the idle packet
    expect_equals 3 publish_packets.size
    resent /mqtt.PublishPacket := publish_packets[0]
    random_topic_packet /mqtt.PublishPacket := publish_packets[1]
    expect_equals topic resent.topic
    expect resent.duplicate
    expect_equals intercepted_packet_id resent.packet_id

    ack_packets := (activity.filter: it[0] == "read" and it[1] is mqtt.PubAckPacket).map: it[1]
    expect_equals 1 ack_packets.size

/**
Tests that the broker resends messages when it hasn't received the client's acks.
*/
test_broker_qos create_transport/Lambda --logger/log.Logger:
  // Create a client that will send a message to the client we are testing.
  with_packet_client create_transport
      --clean_session
      --logger=logger: | other_client/mqtt.FullClient _ _ _ |

    client_id := "test-broker-qos"
    topic := "test-resend"

    // Connect once to clear the session.
    with_packet_client create_transport
        --logger = logger
        --client_id = client_id
        --clean_session:
      null

    created_transport /mqtt.Transport? := null

    // Intercept the transport creation, so we can close the transport from the outside.
    create_transport_intercept := ::
      created_transport = create_transport.call
      created_transport

    // The write filter intercepts the first puback and sets the latch.
    intercepted_latch := Latch

    ack_counter := 0
    write_filter := :: | packet/mqtt.Packet |
      if packet is mqtt.PubAckPacket: ack_counter++
      if ack_counter == 3 and not intercepted_latch.has_value and packet is mqtt.PubAckPacket:
        intercepted_latch.set (packet as mqtt.PubAckPacket).packet_id
        throw "disconnect"
      else:
        packet

    with_packet_client create_transport
        --client_id = client_id
        --write_filter = write_filter
        --no-clean_session
        --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |
      assert: created_transport != null

      client.subscribe topic
      wait_for_idle.call
      clear.call

      other_client.publish topic "increase packet id".to_byte_array --qos=1
      other_client.publish topic "increase packet id".to_byte_array --qos=1

      wait_for_idle.call
      clear.call

      // The other client now sends a message for which the ack is lost,
      // and where the server needs to resend the message.
      other_client.publish topic "ack is lost".to_byte_array --qos=1
      intercepted_packet_id := intercepted_latch.get

      wait_for_idle.call

      activity := get_activity.call
      expect (activity.any: it[0] == "reconnect")

      publish_packets := (activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket).map: it[1]
      // We expect three packets:
      // - the initial packet
      // - the resent packet
      // - the idle packet
      expect_equals 3 publish_packets.size
      original /mqtt.PublishPacket := publish_packets[0]
      resent /mqtt.PublishPacket := publish_packets[1]
      expect_equals topic original.topic
      expect_equals topic resent.topic
      expect_equals original.packet_id resent.packet_id
      expect_not original.duplicate
      expect resent.duplicate
      expect_equals intercepted_packet_id resent.packet_id

      ack_packets := (activity.filter: it[0] == "write" and it[1] is mqtt.PubAckPacket).map: it[1]
      // We immediately resend the ack packet as soon as the connection is established again.
      // However, the broker also sends the same packet again.
      // We end up with two acks for the same packet.
      expect_equals 2 ack_packets.size
      2.repeat:
        expect_equals intercepted_packet_id ack_packets[it].packet_id

test create_transport/Lambda --logger/log.Logger:
  test_clean_session create_transport --logger=logger
  test_subscriptions create_transport --logger=logger
  test_client_qos create_transport --logger=logger
  test_broker_qos create_transport --logger=logger

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  if test_with_mosquitto: with_mosquitto --logger=logger run_test
