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

SUBSCRIPTION_TESTS ::= [
  "##",
  "#/level2",
  "++/level1/level2/level3",
  "le+vel1/level2/level3",
]

PUBLISH_TESTS ::= [
  "#",
  "+/foo",
  "",
]

test_topic topic/string create_transport/Lambda --mode/string --logger/log.Logger:
  transport /mqtt.Transport := create_transport.call
  client := mqtt.FullClient --transport=transport --logger=logger

  options := mqtt.SessionOptions --client_id="test_client"
  client.connect --options=options

  done := monitor.Latch

  callbacks := {:}
  task::
    exception := catch:
      client.handle: | packet/mqtt.Packet |
        if packet is mqtt.PublishPacket:
          client.ack packet
          publish := packet as mqtt.PublishPacket
          callbacks[publish.topic].call publish
        else:
          logger.info "Ignored $(mqtt.Packet.debug_string_ packet)"
      logger.info "client shut down"
    expect_not_null exception
    done.set true

  exception := catch:
    if mode == "subscribe":
      client.subscribe topic
    else if mode == "unsubscribe":
      client.unsubscribe topic
    else if mode == "publish":
      client.publish topic #[]
  if exception:
    logger.info "Caught on client side"
    client.close
    return
  else:
    logger.info "Waiting for client to shut down"
    done.get

/**
Tests that invalid topics are correctly handled.
*/
test create_transport/Lambda --logger/log.Logger:
  SUBSCRIPTION_TESTS.do: | topic |
    test_topic topic create_transport --logger=logger --mode="subscribe"
    test_topic topic create_transport --logger=logger --mode="unsubscribe"
    test_topic topic create_transport --logger=logger --mode="publish"

  PUBLISH_TESTS.do: | topic |
    test_topic topic create_transport --logger=logger --mode="publish"

main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
