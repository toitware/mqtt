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

test_pubsub client/mqtt.Client callbacks/Map --logger/log.Logger:
  2.repeat: | qos |
    TESTS ::= [
      "foo",
      "level1/level2",
      "level1/level2/level3",
      "/leading/empty/level",
      "with spaces",
      ["#", "foobar"],
      ["#", "/foo/bar"],
      ["+", "foo"],
      ["+/", "foo/"],
      ["foo/+/bar", "foo/x/bar"],
      ["foo/+/gee", "foo/y/gee"],
    ]

    TESTS.do: | sub_topic |
      subscription := ?
      topic := ?
      if sub_topic is string:
        subscription = sub_topic
        topic = sub_topic
      else:
        subscription = sub_topic[0]
        topic = sub_topic[1]

      logger.info "Testing topic: $topic - $subscription"

      wait_for_bar := monitor.Latch
      seen_not_bar := false

      callbacks[topic] = :: | packet/mqtt.PublishPacket |
        if packet.payload.to_string == "bar": wait_for_bar.set true
        else: seen_not_bar = true

      client.subscribe subscription
      client.publish topic "not bar".to_byte_array  --qos=qos
      client.publish topic "bar".to_byte_array  --qos=qos

      wait_for_bar.get
      expect seen_not_bar
      client.unsubscribe subscription
      callbacks.remove topic

test_multisub client/mqtt.Client callbacks/Map --logger/log.Logger:
  2.repeat: | max_qos |
    logger.info "****** Testing multi-subscription with max-qos=$max_qos"

    TOPICS ::= [
      "foo/+/gee",
      "#",
      "foo/bar/gee",
    ]

    wait_for_bar := monitor.Latch
    seen_not_bar := false

    callbacks["foo/bar/gee"] = :: | packet/mqtt.PublishPacket |
      if packet.payload.to_string == "bar": wait_for_bar.set true
      else:
        expect_not seen_not_bar
        seen_not_bar = true

    client.subscribe_all
        TOPICS.map: mqtt.TopicFilter it --max_qos=max_qos

    client.publish "foo/bar/gee" "not bar".to_byte_array --qos=1
    client.publish "foo/bar/gee" "bar".to_byte_array --qos=1

    client.unsubscribe_all TOPICS

deserialize

test transport/mqtt.Transport --logger/log.Logger:
  logging_transport := LoggingTransport transport
  logs := logging_transport.log
  client := mqtt.Client --transport=logging_transport --logger=logger

  // No keep-alive pings. As they would make the test non-deterministic.
  options := mqtt.SessionOptions --client_id="test_client" --keep_alive=Duration.ZERO
  client.connect --options=options

  // We are going to use a "idle" ping packet to know when the broker is idle.
  // It's not a guarantee as the broker is allowed to send acks whenever it wants, but
  // it should be quite stable.
  idle := monitor.Semaphore

  client.subscribe "idle" --max_qos=0

  wait_for_idle := :
    client.publish "idle" #[] --qos=0
    idle.down

  task::
    client.handle: | packet/mqtt.Packet |
      logger.info "Received $(stringify_packet packet)"
      if packet is mqtt.PublishPacket:
        client.ack packet
        if (packet as mqtt.PublishPacket).topic == "idle": idle.up

    logger.info "client shut down"

  wait_for_idle.call
  logs.clear

  client.publish "foo/bar/gee" "bar".to_byte_array --qos=1
  wait_for_idle.call
  expect_equals 4 logging_transport.log.size
  expect_equals "write" logs[0][0]  // The publish.
  expect_equals "read" logs[1][0]  // The publish.
  expect Packet




  client.when_running:
    test_pubsub client callbacks --logger=logger
    test_multisub client callbacks --logger=logger

  // TODO(florian): why is this sometimes necessary?
  sleep --ms=10
  client.close

main:
  log_level := log.ERROR_LEVEL
  // log_level := log.DEBUG_LEVEL
  logger := log.Logger log_level TestLogTarget --name="client test"

  run_test := : | transport | test transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
