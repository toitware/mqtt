// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import monitor
import net

import .broker_internal
import .broker_mosquitto
import .transport
import .packet_test_client

/**
Tests that the client can handle acks that arrive fast. That is, where
  the client is still in the 'send' routine of the transport.
*/
main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.DEBUG_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  if test_with_mosquitto: with_mosquitto --logger=logger run_test
  else: with_internal_broker --logger=logger run_test

class TestPersistenceStore extends mqtt.MemoryPersistenceStore:
  added := {}
  removed := {}

  add packet/mqtt.PersistedPacket -> none:
    added.add packet.packet_id
    super packet

  remove_persisted_with_id packet_id/int -> bool:
    removed.add packet_id
    return super packet_id

test create_transport/Lambda --logger/log.Logger:
  persistence_store := TestPersistenceStore
  id := "persistence_client_id"

  return_from_write := monitor.Channel 1
  delay_is_active := false
  create_test_transport := ::
    test_transport := CallbackTestTransport create_transport.call
    test_transport.on_after_write = ::
      if delay_is_active: return_from_write.receive
    test_transport

  received_ack := monitor.Channel 1
  read_filter := :: | packet/mqtt.Packet |
    if packet is mqtt.PubAckPacket:
      received_ack.send true
    packet

  with_packet_client create_test_transport
      --client_id = id
      --read_filter = read_filter
      --persistence_store = persistence_store
      --logger=logger: | client/mqtt.FullClient wait_for_idle/Lambda _ _ |

    print "wait for idle"
    wait_for_idle.call

    client.publish "not_delayed" "payload".to_byte_array --qos=1
    received_ack.receive
    YIELD_COUNT ::= 10
    YIELD_COUNT.repeat: yield
    expect persistence_store.is_empty
    // The ack should have made it to the persistence store.
    expect persistence_store.is_empty
    expect_equals 1 persistence_store.added.size

    delay_is_active = true
    publish_is_done := false
    task::
      client.publish "delayed" "payload".to_byte_array --qos=1
      publish_is_done = true
    // The delay is *after* we sent the message to the broker.
    // We expect to get an ack.
    received_ack.receive
    // Allow the ack to propagate.
    YIELD_COUNT.repeat: yield
    // The publish is still in process.
    expect_not publish_is_done
    // At this point the persistence store hasn't received the packet
    // nor the ack yet as the send routine is still running.
    expect persistence_store.is_empty
    expect_equals 1 persistence_store.added.size

    return_from_write.send true
    YIELD_COUNT.repeat: yield
    // The publish is now done.
    expect publish_is_done
    // The ack should have made it to the persistence store.
    expect persistence_store.is_empty
    expect_equals 2 persistence_store.added.size
    expect_equals 2 persistence_store.removed.size

    delay_is_active = false
