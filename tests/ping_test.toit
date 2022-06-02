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
import .packet_test_client

/**
Tests that the client and broker correctly ack packets.
*/
test create_transport/Lambda logger/log.Logger:
  options := mqtt.SessionOptions --client_id="test_client"
      --keep_alive=(Duration --s=1)
      --clean_session
  with_packet_client create_transport
      --options=options
      --logger=logger : | client/mqtt.Client _ _ get_packets/Lambda |
    sleep --ms=2_000

    packets := get_packets.call
    sent_ping := packets.any:
      it[0] == "write" and it[1] is mqtt.PingReqPacket
    expect sent_ping

main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  with_internal_broker --logger=logger: test it logger
  with_mosquitto --logger=logger: test it logger
