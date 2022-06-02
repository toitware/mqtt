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
Tests that two clients can communicate through the broker.
*/
test create_transport/Lambda --logger/log.Logger:
  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  keep_alive := Duration --s=10_000

  transport1 /mqtt.Transport := create_transport.call
  client1 := mqtt.Client --transport=transport1 --logger=logger
  options1 := mqtt.SessionOptions --client_id="test_client1" --keep_alive=keep_alive --clean_session
  client1.connect --options=options1

  transport2 /mqtt.Transport := create_transport.call
  client2 := mqtt.Client --transport=transport2 --logger=logger
  options2 := mqtt.SessionOptions --client_id="test_client2" --keep_alive=keep_alive --clean_session
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
        client2.publish "2-to-1" "pong".to_byte_array --qos=qos

  2.repeat:
    qos = it
    ping_pong_count = 0
    if it == 0: client1.publish "1-to-2" "ping".to_byte_array --qos=qos
    else: client2.publish "2-to-1" "ping".to_byte_array --qos=qos
    done.down
    done.down

  client1.close
  client2.close

main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
