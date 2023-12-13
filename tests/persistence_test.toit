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
Tests that the persistence store stores unsent packets, and that a new
  client can reuse that persistence store.
*/
test create-transport/Lambda --logger/log.Logger:
  persistence-store := mqtt.MemoryPersistenceStore
  id := "persistence_client_id"

  intercepting-writing := monitor.Latch
  write-filter := :: | packet/mqtt.Packet |
    if packet is mqtt.PublishPacket:
      publish := packet as mqtt.PublishPacket
      if publish.topic == "to_be_intercepted":
        intercepting-writing.set true
    if intercepting-writing.has-value: null
    else: packet

  with-packet-client create-transport
      --client-id = id
      --write-filter = write-filter
      --persistence-store = persistence-store
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda _ _ |

    // Use up one packet id.
    client.publish "not_intercepted" "payload".to-byte-array --qos=1

    wait-for-idle.call

    // The write-filter will not let this packet through and stop every future write.
    client.publish "to_be_intercepted" "payload".to-byte-array --qos=1

    intercepting-writing.get

    client.close --force

    expect-equals 1 persistence-store.size

  // Delay ack packets that come back from the broker.
  // This is to ensure that we don't reuse IDs that haven't been
  // acked yet.
  release-ack-packets := monitor.Latch
  ack-ids := {}
  read-filter := :: | packet/mqtt.Packet |
    if packet is mqtt.PubAckPacket:
      release-ack-packets.get
      ack-ids.add (packet as mqtt.PubAckPacket).packet-id
    packet

  // We reconnect with a new client reusing the same persistence store.
  with-packet-client create-transport
      --client-id = id
      --persistence-store = persistence-store
      --read-filter = read-filter
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda _ get-activity/Lambda |

    client.publish "not_intercepted1" "another payload".to-byte-array --qos=1
    client.publish "not_intercepted2" "another payload2".to-byte-array --qos=1
    client.publish "not_intercepted3" "another payload3".to-byte-array --qos=1
    release-ack-packets.set true
    wait-for-idle.call
    activity /List := get-activity.call
    client.close

    // Check that no packet-id was reused and we have 4 different acks.
    expect-equals 4 ack-ids.size

    expect persistence-store.is-empty

    // We check that the persisted packet is now sent and removed from the store.
    publish-packets := (activity.filter: it[0] == "write" and it[1] is mqtt.PublishPacket).map: it[1]
    publish-packets.filter --in-place: it.topic == "to_be_intercepted"
    expect-equals 1 publish-packets.size

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
