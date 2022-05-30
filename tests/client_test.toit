// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import host.pipe
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker as broker
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
    logger.info "Testing multi-subscription with max-qos=$max_qos"

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

test transport/mqtt.Transport logger/log.Logger:
  client := mqtt.Client --transport=transport --logger=logger


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
        logger.info "Ignored $(stringify_packet packet)"
    logger.info "client shut down"

  client.when_running:
    test_pubsub client callbacks --logger=logger
    // test_multisub client callbacks --logger=logger

  // TODO(florian): why is this sometimes necessary?
  sleep --ms=10
  client.close

start_mosquitto:
  port /string := pipe.backticks "python" "third_party/ephemeral-port-reserve/ephemeral_port_reserve.py"
  port = port.trim
  fork_data := pipe.fork
      true  // use_path.
      pipe.PIPE_INHERITED  // stdin.
      pipe.PIPE_CREATED  // stdout.
      pipe.PIPE_CREATED  // stderr.
      "mosquitto"  // Program.
      ["mosquitto", "-v", "-p", port]  // Args.
  return [
    int.parse port,
    fork_data
  ]

test_with_internal logger/log.Logger:

  server_transport := TestServerTransport
  broker := broker.Broker server_transport --logger=logger
  client_transport := TestClientTransport server_transport
  broker_task := task:: broker.start

  test client_transport logger

  broker_task.cancel

test_with_mosquitto logger/log.Logger:
  mosquitto_data := start_mosquitto
  port := mosquitto_data[0]
  logger.info "Started Mosquitto on port $port"

  mosquitto_fork_data := mosquitto_data[1]

  mosquitto_is_running := monitor.Latch
  stdout_bytes := #[]
  stderr_bytes := #[]
  task::
    stdout /pipe.OpenPipe := mosquitto_fork_data[1]
    while chunk := stdout.read:
      logger.debug chunk.to_string.trim
      stdout_bytes += chunk
  task::
    stderr /pipe.OpenPipe := mosquitto_fork_data[2]
    while chunk := stderr.read:
      str := chunk.to_string.trim
      logger.debug str
      stderr_bytes += chunk
      if str.contains "mosquitto version" and str.contains "running":
        mosquitto_is_running.set true

  mosquitto_is_running.get

  network := net.open
  transport := mqtt.TcpTransport network --host="localhost" --port=port

  try:
    test transport logger
  finally: | is_exception _ |
    pid := mosquitto_fork_data[3]
    logger.info "Killing mosquitto server"
    pipe.kill_ pid 15
    pipe.wait_for pid
    if is_exception:
      print stdout_bytes.to_string
      print stderr_bytes.to_string

/**
Function to test with an external mosquitto.
Can sometimes be useful, as the logging is better.
*/
test_with_external_mosquitto logger/log.Logger:
  network := net.open
  transport := mqtt.TcpTransport network --host="localhost" --port=1883
  test transport logger

main:
  log_level := log.ERROR_LEVEL
  // log_level := log.INFO_LEVEL
  logger := log.Logger log_level TestLogTarget --name="client test"

  test_with_internal logger
  test_with_mosquitto logger
