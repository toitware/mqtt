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

PING_PONG_MAX ::= 3

/**
Tests that the client and broker correctly ack packets.
*/
test transport/mqtt.Transport --logger/log.Logger:
  client1 := mqtt.Client --transport=transport --logger=logger
  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  options1 := mqtt.SessionOptions --client_id="test_client1" --keep_alive=(Duration --s=10000)
      --clean_session
  client1.connect --options=options1

  client2 := mqtt.Client --transport=transport --logger=logger

  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  options2 := mqtt.SessionOptions --client_id="test_client2" --keep_alive=(Duration --s=10000)
      --clean_session

  client2.connect --options=options2

  client1_callback /Lambda := :: it // Ignore the packet.
  client2_callback /Lambda := :: it // Ignore the packet.

  2.repeat: | client_index |
    client := client_index == 0 ? client1 : client2
    task::
      client.handle: | packet/mqtt.Packet |
        logger.info "Received $(mqtt.Packet.debug_string_ packet)"
        if client_index == 0: client1_callback.call packet
        else: client2_callback.call packet

      logger.info "client$(client_index + 1) shut down"

  // Wait for both clients to be connected.
  client1.when_running: null
  client2.when_running: null

  client1.subscribe "2-to-1"
  client2.subscribe "1-to-2"

  done := monitor.Semaphore

  qos := 0

  ping_pong_count := 0
  client1_callback = :: | packet |
    client1.ack packet
    if packet is mqtt.PublishPacket:
      ping_pong_count++
      if ping_pong_count >= PING_PONG_MAX: done.up
      if ping_pong_count <= PING_PONG_MAX:
        client1.publish "1-to-2" "ping".to_byte_array --qos=qos

  client2_callback = :: | packet |
    client2.ack packet
    if packet is mqtt.PublishPacket:
      ping_pong_count++
      if ping_pong_count >= PING_PONG_MAX: done.up
      if ping_pong_count <= PING_PONG_MAX:
        client1.publish "2-to-1" "pong".to_byte_array --qos=qos

  2.repeat:
    qos = it
    ping_pong_count = 0
    client1.publish "1-to-2" "ping".to_byte_array --qos=qos
    done.down
    done.down

  sleep --ms=100
  client1.close
  client2.close

main:
  // log_level := log.ERROR_LEVEL
  log_level := log.DEBUG_LEVEL
  logger := log.Logger log_level TestLogTarget --name="client test"

  run_test := : | transport | test transport --logger=logger
  // with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
