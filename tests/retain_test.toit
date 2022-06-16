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
  topic := "test/retain"
  with_packet_client create_transport
      --logger=logger : | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    client.subscribe topic
    client.publish topic "test".to_byte_array
    wait_for_idle.call

    clear.call
    client.subscribe topic
    wait_for_idle.call

    // 2 messages for the subscription.
    expect_equals 2 get_activity.call.size

    2.repeat: | qos |
      // Now send a packet with retain.
      // We then subscribe again to the same topic.
      // This time, we should receive the same packet, but with a 'retain' flag set.
      client.publish topic "test".to_byte_array --retain --qos=qos
      wait_for_idle.call

      clear.call
      client.subscribe topic
      wait_for_idle.call

      activity := get_activity.call
      // 2 messages for the subscription.
      // 1 or 2 for the retained packet. (depending on qos)
      expected_count := qos == 0 ? 3 : 4
      expect_equals expected_count activity.size
      reads := activity.filter: it[0] == "read"
      writes := activity.filter: it[0] == "write"
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
    // No retained packet.
    expect_equals 2 get_activity.call.size

    // Check that other clients also get the retained message.

    client.publish topic "available for other clients".to_byte_array --qos=0 --retain

    with_packet_client create_transport
        --client_id = "other client"
        --logger = logger:
      | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

      clear.call
      client.subscribe topic
      wait_for_idle.call

      activity := get_activity.call
      reads := activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
      retained /mqtt.PublishPacket := reads.first[1]
      expect_equals topic retained.topic
      expect_equals "available for other clients" retained.payload.to_string
      expect retained.retain

  // The retained message stays even when the original sender has died.
  with_packet_client create_transport
      --client_id = "third client"
      --logger = logger:
    | client/mqtt.FullClient wait_for_idle/Lambda clear/Lambda get_activity/Lambda |

    clear.call
    client.subscribe topic
    wait_for_idle.call

    activity := get_activity.call
    reads := activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
    retained /mqtt.PublishPacket := reads.first[1]
    expect_equals topic retained.topic
    expect_equals "available for other clients" retained.payload.to_string
    expect retained.retain


main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  if test_with_mosquitto: with_mosquitto --logger=logger run_test
  else: with_internal_broker --logger=logger run_test
