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

PING_PONG_MAX ::= 100

/**
Tests the last-will message.
*/
test create_transport/Lambda --logger/log.Logger:
  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  keep_alive := Duration --s=10_000

  clients := []
  transports := []

  id_counter := 0
  2.repeat: | qos |
    2.repeat: | retain_counter |
      retain := retain_counter == 0
      last_will := mqtt.LastWill --qos=qos --retain=retain
          "test/last_will$id_counter"
          "last_will_message $qos $retain".to_byte_array
      transport /mqtt.Transport := create_transport.call
      transports.add transport
      client := mqtt.FullClient --transport=transport --logger=logger
      options := mqtt.SessionOptions --client_id="test_client $(id_counter++)"
          --keep_alive=keep_alive
          --clean_session
          --last_will=last_will
      client.connect --options=options
      clients.add client

  receiver_transport /mqtt.Transport := create_transport.call
  receiver_client := mqtt.FullClient --transport=receiver_transport --logger=logger
  receiver_options := mqtt.SessionOptions --client_id="receiver" --keep_alive=keep_alive --clean_session
  receiver_client.connect --options=receiver_options

  clients.do: | client |
    task::
      catch:
        client.handle: | packet/mqtt.Packet |
          logger.info "received $(mqtt.Packet.debug_string_ packet)"
          client.ack packet

        logger.info "client shut down"

  subscription_ack_semaphore := monitor.Semaphore

  received := monitor.Semaphore
  received_packets := {:}
  task::
    receiver_client.handle: | packet/mqtt.Packet |
      logger.info "received $(mqtt.Packet.debug_string_ packet)"
      receiver_client.ack packet
      if packet is mqtt.PublishPacket:
        received.up
        publish := packet as mqtt.PublishPacket
        received_packets[publish.topic] = publish
      else if packet is mqtt.SubAckPacket:
        subscription_ack_semaphore.up

    logger.info "receiver client shut down"

  // Wait for all clients to be connected.
  clients.do: it.when_running: null
  receiver_client.when_running: null

  receiver_client.subscribe "test/last_will0"
  receiver_client.subscribe "test/last_will1"
  receiver_client.subscribe "test/last_will2"
  receiver_client.subscribe "test/last_will3"

  subscription_ack_semaphore.down
  subscription_ack_semaphore.down
  subscription_ack_semaphore.down
  subscription_ack_semaphore.down

  // Cut the connections of the clients, triggering a last will.
  transports.do: it.close

  4.repeat: received.down

  4.repeat:
    packet /mqtt.PublishPacket := received_packets["test/last_will$it"]
    payload := packet.payload.to_string
    parts := payload.split " "
    expected_qos := int.parse parts[1]
    if expected_qos == 0: expect_null packet.packet_id
    else: expect_not_null packet.packet_id
    // Since we were alive when the last will was sent, the retain is set to false.
    expect_not packet.retain

  // Reset.
  received_packets.clear

  // Subscribe again to get the retained messages.
  receiver_client.subscribe "test/last_will0"
  receiver_client.subscribe "test/last_will1"
  receiver_client.subscribe "test/last_will2"
  receiver_client.subscribe "test/last_will3"

  2.repeat: received.down
  received_packets.do: | topic packet/mqtt.PublishPacket |
    payload := packet.payload.to_string
    parts := payload.split " "
    expected_qos := int.parse parts[1]
    if expected_qos == 0: expect_null packet.packet_id
    else: expect_not_null packet.packet_id
    expected_retain := parts[2] == "true"
    expect expected_retain
    // Since we subscribed again, the retain flag must be set.
    expect packet.retain

  receiver_client.close

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  if test_with_mosquitto: with_mosquitto --logger=logger run_test
