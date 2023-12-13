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

import .broker-internal
import .broker-mosquitto
import .transport
import .packet-test-client

/**
Tests that the client can handle acks that arrive fast. That is, where
  the client is still in the 'send' routine of the transport.
*/
main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.DEBUG-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test

class TestPersistenceStore extends mqtt.MemoryPersistenceStore:
  added := {}
  removed := {}

  add packet/mqtt.PersistedPacket -> none:
    added.add packet.packet-id
    super packet

  remove-persisted-with-id packet-id/int -> bool:
    removed.add packet-id
    return super packet-id

test create-transport/Lambda --logger/log.Logger:
  persistence-store := TestPersistenceStore
  id := "persistence_client_id"

  return-from-write := monitor.Channel 1
  delay-is-active := false
  create-test-transport := ::
    test-transport := CallbackTestTransport create-transport.call
    test-transport.on-after-write = ::
      if delay-is-active: return-from-write.receive
    test-transport

  received-ack := monitor.Channel 1
  read-filter := :: | packet/mqtt.Packet |
    if packet is mqtt.PubAckPacket:
      received-ack.send true
    packet

  with-packet-client create-test-transport
      --client-id = id
      --read-filter = read-filter
      --persistence-store = persistence-store
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda _ _ |

    print "wait for idle"
    wait-for-idle.call

    client.publish "not_delayed" "payload".to-byte-array --qos=1
    received-ack.receive
    YIELD-COUNT ::= 10
    YIELD-COUNT.repeat: yield
    expect persistence-store.is-empty
    // The ack should have made it to the persistence store.
    expect persistence-store.is-empty
    expect-equals 1 persistence-store.added.size

    delay-is-active = true
    publish-is-done := false
    task::
      client.publish "delayed" "payload".to-byte-array --qos=1
      publish-is-done = true
    // The delay is *after* we sent the message to the broker.
    // We expect to get an ack.
    received-ack.receive
    // Allow the ack to propagate.
    YIELD-COUNT.repeat: yield
    // The publish is still in process.
    expect-not publish-is-done
    // At this point the persistence store hasn't received the packet
    // nor the ack yet as the send routine is still running.
    expect persistence-store.is-empty
    expect-equals 1 persistence-store.added.size

    return-from-write.send true
    YIELD-COUNT.repeat: yield
    // The publish is now done.
    expect publish-is-done
    // The ack should have made it to the persistence store.
    expect persistence-store.is-empty
    expect-equals 2 persistence-store.added.size
    expect-equals 2 persistence-store.removed.size

    delay-is-active = false
