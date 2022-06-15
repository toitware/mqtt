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

test_pubsub client/mqtt.FullClient callbacks/Map --logger/log.Logger:
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

      logger.info "testing topic: $topic - $subscription"

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

  // Test sending a message in the handle function.
  topic := "in_handle"
  response_topic := "response_topic"
  client.subscribe topic
  client.subscribe response_topic

  callbacks[topic] = :: | packet/mqtt.PublishPacket |
    client.publish response_topic "response".to_byte_array

  got_response := monitor.Latch
  callbacks[response_topic] = :: | packet/mqtt.PublishPacket |
    client.unsubscribe topic
    client.unsubscribe response_topic
    got_response.set true

  client.publish topic "message".to_byte_array
  got_response.get

test_multisub client/mqtt.FullClient callbacks/Map --logger/log.Logger:
  client.subscribe "idle"
  2.repeat: | max_qos |
    logger.info "testing multi-subscription with max-qos=$max_qos"

    TOPICS ::= [
      "foo/+/gee",
      "#",
      "foo/bar/gee",
    ]

    wait_for_bar := monitor.Latch
    idle := monitor.Semaphore
    seen_not_bar := false

    callbacks["idle"] = :: | packet/mqtt.PublishPacket |
      idle.up

    callbacks["foo/bar/gee"] = :: | packet/mqtt.PublishPacket |
      if packet.payload.to_string == "bar": wait_for_bar.set true
      else:
        expect_not seen_not_bar
        seen_not_bar = true

    client.subscribe_all
        TOPICS.map: mqtt.TopicQos it --max_qos=max_qos

    client.publish "foo/bar/gee" "not bar".to_byte_array --qos=1
    client.publish "foo/bar/gee" "bar".to_byte_array --qos=1

    client.unsubscribe_all TOPICS
    client.publish "idle" #[] --qos=0
    idle.down

test create_transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create_transport.call
  client := mqtt.FullClient --transport=transport --logger=logger

  options := mqtt.SessionOptions --client_id="test_client"
  client.connect --options=options

  callbacks := {:}
  task::
    client.handle: | packet/mqtt.Packet |
      if packet is mqtt.PublishPacket:
        client.ack packet
        publish := packet as mqtt.PublishPacket
        callbacks[publish.topic].call publish
      else:
        logger.info "ignored $(mqtt.Packet.debug_string_ packet)"
    logger.info "client shut down"

  client.when_running:
    test_pubsub client callbacks --logger=logger
    test_multisub client callbacks --logger=logger

  client.close

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  if test_with_mosquitto: with_mosquitto --logger=logger run_test
