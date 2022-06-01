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
import .log
import .transport

test_publish client/mqtt.Client transport/LoggingTransport --auto_ack_enabled/bool --logger/log.Logger [--wait_for_idle]:
  2.repeat: | qos |
    transport.clear
    client.publish "foo/bar/gee" "bar".to_byte_array --qos=qos
    wait_for_idle.call

    logs := transport.packets
    expected_count := qos == 0 ? 3 : 4
    expect_equals expected_count logs.size
    expect_equals "write" logs[0][0]  // The publish.
    publish := logs[0][1] as mqtt.PublishPacket
    if qos == 0:
      expect_null publish.packet_id
    else:
      response := logs[1][0] == "read" ? logs[1] : logs[2]
      ack := response[1] as mqtt.PubAckPacket
      expect publish.packet_id == ack.packet_id
    // The other 2 packets are the idle packets.

test_sub_unsub client/mqtt.Client transport/LoggingTransport --logger/log.Logger [--wait_for_idle]:
  // Subscriptions always have qos=1.
  transport.clear
  client.subscribe "foo/bar"
  wait_for_idle.call

  logs := transport.packets
  expect_equals 4 logs.size
  expect_equals "write" logs[0][0]  // The subscription.
  subscribe := logs[0][1] as mqtt.SubscribePacket
  response := logs[1][0] == "read" ? logs[1] : logs[2]
  sub_ack := response[1] as mqtt.SubAckPacket
  expect subscribe.packet_id == sub_ack.packet_id
  // The other 2 packets are the idle packets.

  // Unsubscriptions always have qos=1.
  transport.clear
  client.unsubscribe "foo/bar"
  wait_for_idle.call

  logs = transport.packets
  expect_equals 4 logs.size
  expect_equals "write" logs[0][0]  // The subscription.
  unsubscribe := logs[0][1] as mqtt.UnsubscribePacket
  response = logs[1][0] == "read" ? logs[1] : logs[2]
  unsub_ack := response[1] as mqtt.UnsubAckPacket
  expect unsubscribe.packet_id == unsub_ack.packet_id
  // The other 2 packets are the idle packets.

/**
Tests that the client and broker correctly ack packets.
*/
test transport/mqtt.Transport --logger/log.Logger:
  logging_transport := LoggingTransport transport
  client := mqtt.Client --transport=logging_transport --logger=logger

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
      logger.info "Received $(mqtt.Packet.debug_string_ packet)"
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

  client.close

main:
  log_level := log.ERROR_LEVEL
  // log_level := log.DEBUG_LEVEL
  logger := log.Logger log_level TestLogTarget --name="client test"

  run_test := : | transport | test transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
  // with_external_mosquitto --logger=logger run_test
