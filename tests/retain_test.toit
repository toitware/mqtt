// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker_internal
import .broker_mosquitto
import .packet_test_client
import .transport

/**
Tests that the client and broker correctly ack packets.
*/
test create_transport/Lambda --logger/log.Logger:
  with_packet_client create_transport
      --logger=logger : | client/mqtt.Client wait_for_idle/Lambda clear/Lambda get_packets/Lambda |

    topic := "test/retain"
    client.subscribe topic
    client.publish topic "test".to_byte_array
    wait_for_idle.call

    clear.call
    client.subscribe topic
    wait_for_idle.call

    // 2 messages for the subscription.
    // 2 for the idle.
    expect_equals 4 get_packets.call.size

    2.repeat: | qos |
      // Now send a packet with retain.
      // We then subscribe again to the same topic.
      // This time, we should receive the same packet, but with a 'retain' flag set.
      client.publish topic "test".to_byte_array --retain --qos=qos
      wait_for_idle.call

      clear.call
      client.subscribe topic
      wait_for_idle.call

      packets := get_packets.call
      // 2 messages for the subscription.
      // 1 or 2 for the retained packet. (depending on qos)
      // 2 for the idle.
      expected_count := qos == 0 ? 5 : 6
      expect_equals expected_count packets.size
      reads := packets.filter: it[0] == "read"
      writes := packets.filter: it[0] == "write"
      publish := (reads.filter: it[1] is mqtt.PublishPacket)[0][1]
      expect_equals topic publish.topic
      if qos == 0: expect_null publish.packet_id
      else: expect_not_null publish.packet_id
      expect publish.retain

    // A message with 0 bytes drops the retained packet.
    client.publish topic #[] --retain
    wait_for_idle.call

    clear.call
    client.subscribe topic
    wait_for_idle.call

    // 2 messages for the subscription.
    // 2 for the idle.
    // No retained packet.
    expect_equals 4 get_packets.call.size

main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
