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
Tests that the persistence store stores unsent packets, and that a new
  client can reuse that persistence store.
*/
test create_transport/Lambda --logger/log.Logger:
  persistence_store := mqtt.MemoryPersistenceStore
  id := "persistence_client_id"

  intercepting_writing := monitor.Latch
  write_filter := :: | packet/mqtt.Packet |
    if packet is mqtt.PublishPacket:
      publish := packet as mqtt.PublishPacket
      if publish.topic == "to_be_intercepted":
        intercepting_writing.set true
    if intercepting_writing.has_value: null
    else: packet

  with_packet_client create_transport
      --client_id = id
      --write_filter = write_filter
      --persistence_store = persistence_store
      --logger=logger: | client/mqtt.FullClient _ _ _ |

    // The write-filter will not let this packet through and stop every future write.
    client.publish "to_be_intercepted" "payload".to_byte_array --qos=1

    intercepting_writing.get

    client.close --force

    expect_equals 1 persistence_store.size

  // We reconnect with a new client reusing the same persistence store.
  with_packet_client create_transport
      --client_id = id
      --persistence_store = persistence_store
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda _ get_activity/Lambda |

    wait_for_idle.call
    activity /List := get_activity.call
    client.close

    expect persistence_store.is_empty

    // We check that the persisted packet is now sent and removed from the store.
    publish_packets := (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).map: it[1]
    publish_packets.filter --in_place: it.topic == "to_be_intercepted"
    expect_equals 1 publish_packets.size

main:
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  with_mosquitto --logger=logger run_test
