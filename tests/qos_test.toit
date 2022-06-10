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

test_publish client/mqtt.FullClient transport/TestTransport --auto_ack_enabled/bool --logger/log.Logger [--wait_for_idle]:
  2.repeat: | qos |
    transport.clear
    client.publish "foo/bar/gee" "bar".to_byte_array --qos=qos
    wait_for_idle.call

    activity := transport.activity
    expected_count := qos == 0 ? 3 : 4
    expect_equals expected_count activity.size
    expect_equals "write" activity[0][0]  // The publish.
    publish := activity[0][1] as mqtt.PublishPacket
    if qos == 0:
      expect_null publish.packet_id
    else:
      response := activity[1][0] == "read" ? activity[1] : activity[2]
      ack := response[1] as mqtt.PubAckPacket
      expect publish.packet_id == ack.packet_id
    // The other 2 packets are the idle packets.

test_sub_unsub client/mqtt.FullClient transport/TestTransport --logger/log.Logger [--wait_for_idle]:
  // Subscriptions always have qos=1.
  transport.clear
  client.subscribe "foo/bar"
  wait_for_idle.call

  activity := transport.activity
  expect_equals 4 activity.size
  expect_equals "write" activity[0][0]  // The subscription.
  subscribe := activity[0][1] as mqtt.SubscribePacket
  response := activity[1][0] == "read" ? activity[1] : activity[2]
  sub_ack := response[1] as mqtt.SubAckPacket
  expect subscribe.packet_id == sub_ack.packet_id
  // The other 2 packets are the idle packets.

  // Unsubscriptions always have qos=1.
  transport.clear
  client.unsubscribe "foo/bar"
  wait_for_idle.call

  activity = transport.activity
  expect_equals 4 activity.size
  expect_equals "write" activity[0][0]  // The subscription.
  unsubscribe := activity[0][1] as mqtt.UnsubscribePacket
  response = activity[1][0] == "read" ? activity[1] : activity[2]
  unsub_ack := response[1] as mqtt.UnsubAckPacket
  expect unsubscribe.packet_id == unsub_ack.packet_id
  // The other 2 packets are the idle packets.

test_max_qos client/mqtt.FullClient transport/TestTransport --logger/log.Logger [--wait_for_idle]:
  2.repeat: | max_qos |
    topic := "foo/bar$max_qos"
    client.subscribe topic --max_qos=max_qos
    wait_for_idle.call

    2.repeat: | packet_qos |
      transport.clear
      client.publish topic "bar".to_byte_array --qos=packet_qos
      wait_for_idle.call

      expect_count := 4
      if packet_qos != 0:
        // If the packet_qos is 0, then there will never be an ack.
        expect_count++
        // If the packet qos is 1, then there might be another ack to the sub.
        if max_qos != 0: expect_count++

      activity := transport.activity
      expect_equals expect_count activity.size
      reads := activity.filter: it[0] == "read"
      writes := activity.filter: it[0] == "write"

      to_broker := writes[0][1] as mqtt.PublishPacket
      expect_equals topic to_broker.topic
      if packet_qos == 0:
        expect_null to_broker.packet_id
      else:
        to_broker_ack := (reads.filter: it[1] is mqtt.PubAckPacket)[0][1]
        expect_equals to_broker.packet_id to_broker_ack.packet_id

      from_broker := (reads.filter: it[1] is mqtt.PublishPacket)[0][1]
      if packet_qos == 0 or max_qos == 0:
        expect_null from_broker.packet_id
      else:
        from_broker_ack := (writes.filter: it[1] is mqtt.PubAckPacket)[0][1]
        expect_equals from_broker.packet_id from_broker_ack.packet_id

/**
Tests that the client and broker correctly ack packets.
*/
test create_transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create_transport.call
  logging_transport := TestTransport transport
  client := mqtt.FullClient --transport=logging_transport --logger=logger

  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  options := mqtt.SessionOptions --client_id="test_client" --keep_alive=(Duration --s=10_000)
      --clean_session
  client.connect --options=options

  // We are going to use a "idle" ping packet to know when the broker is idle.
  // It's not a guarantee as the broker is allowed to send acks whenever it wants, but
  // it should be quite stable.
  idle := monitor.Semaphore

  wait_for_idle := :
    client.publish "idle" #[] --qos=0
    idle.down

  auto_ack := true

  task::
    client.handle: | packet/mqtt.Packet |
      logger.info "received $(mqtt.Packet.debug_string_ packet)"
      if packet is mqtt.PublishPacket:
        if not auto_ack: client.ack packet
        if (packet as mqtt.PublishPacket).topic == "idle": idle.up

    logger.info "client shut down"

  client.when_running:
    client.subscribe "idle" --max_qos=0
    wait_for_idle.call

  2.repeat:
    test_publish client logging_transport
        --logger=logger
        --auto_ack_enabled=auto_ack
        --wait_for_idle=wait_for_idle
    // Disable auto-ack for the next iteration and the remaining tests.
    auto_ack = false


  test_sub_unsub client logging_transport --logger=logger --wait_for_idle=wait_for_idle
  test_max_qos client logging_transport --logger=logger --wait_for_idle=wait_for_idle

  client.close

main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
