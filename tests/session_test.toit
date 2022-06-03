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
import .packet_test_client
import .transport


test_clean_session create_transport/Lambda --logger/log.Logger:
  // First connection should be clean.
  with_packet_client create_transport
      --logger=logger : | client/mqtt.Client wait_for_idle/Lambda clear/Lambda get_packets/Lambda |
    packets := get_packets.call
    connect_ack /mqtt.ConnAckPacket := (packets.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

  // Second connection should be clean too.
  with_packet_client create_transport
      --logger=logger : | client/mqtt.Client wait_for_idle/Lambda clear/Lambda get_packets/Lambda |
    packets := get_packets.call
    connect_ack /mqtt.ConnAckPacket := (packets.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

  // Third connection should be clean, but we now connect without clean_session.
  with_packet_client create_transport
      --no-clean_session
      --logger=logger : | client/mqtt.Client wait_for_idle/Lambda clear/Lambda get_packets/Lambda |
    packets := get_packets.call
    connect_ack /mqtt.ConnAckPacket := (packets.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

  // Now we should have a session.
  with_packet_client create_transport
      --no-clean_session
      --logger=logger : | client/mqtt.Client wait_for_idle/Lambda clear/Lambda get_packets/Lambda |
    packets := get_packets.call
    connect_ack /mqtt.ConnAckPacket := (packets.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect connect_ack.session_present

  // Connecting again with a clean-session flag yields in no-session again.
  // Third connection should be clean, but we now connect without clean_session.
  with_packet_client create_transport
      --logger=logger : | client/mqtt.Client wait_for_idle/Lambda clear/Lambda get_packets/Lambda |
    packets := get_packets.call
    connect_ack /mqtt.ConnAckPacket := (packets.filter: it[0] == "read" and it[1] is mqtt.ConnAckPacket).first[1]
    expect_not connect_ack.session_present

test create_transport/Lambda --logger/log.Logger:
  test_clean_session create_transport --logger=logger


main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
