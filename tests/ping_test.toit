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

/**
Tests that the client and broker correctly ack packets.
*/
test create_transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create_transport.call
  logging_transport := LoggingTransport transport
  client := mqtt.Client --transport=logging_transport --logger=logger

  options := mqtt.SessionOptions --client_id="test_client" --keep_alive=(Duration --s=1)
      --clean_session
  client.connect --options=options

  sleep --ms=2_000

  packets := logging_transport.packets
  sent_ping := packets.any:
    if it[0] == "write": print (mqtt.Packet.debug_string_ it[1])
    it[0] == "write" and it[1] is mqtt.PingReqPacket
  expect sent_ping

  client.close

main:
  log_level := log.ERROR_LEVEL
  logger := log.Logger log_level TestLogTarget --name="client test"

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
