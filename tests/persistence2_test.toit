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

class TestPersistenceStore implements mqtt.PersistenceStore:
  packets := {:}
  acked := []

  add packet/mqtt.PersistedPacket -> none:
    packets[packet.packet-id] = packet

  get packet-id/int -> mqtt.PersistedPacket?:
    return packets.get packet-id

  remove-persisted-with-id packet-id/int -> bool:
    old := packets.get packet-id
    if old:
      acked.add old
    packets.remove packet-id
    return old != null

  do [block] -> none:
    packets.do --values block

  size -> int:
    return packets.size


/**
Tests that the persistence store stores unsent packets, and that a new
  client can reuse that persistence store.
*/
test
    create-transport/Lambda
    --logger/log.Logger:

  persistence-store := TestPersistenceStore
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
      --no-clean-session
      --write-filter = write-filter
      --persistence-store = persistence-store
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda _ _ |

    // Use up one packet id.
    client.publish "not_intercepted" "payload".to-byte-array
        --qos=1
        --persistence-token="not_intercepted"

    wait-for-idle.call

    // The write-filter will not let this packet through and stop every future write.
    client.publish "to_be_intercepted" "payload".to-byte-array
        --qos=1
        --persistence-token="to_be_intercepted"

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
      --no-clean-session
      --persistence-store = persistence-store
      --read-filter = read-filter
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda _ get-activity/Lambda |

    client.publish "not_intercepted1" "another payload".to-byte-array
        --qos=1
        --persistence-token="not_intercepted1"
    client.publish "not_intercepted2" "another payload2".to-byte-array
        --qos=1
        --persistence-token="not_intercepted2"
    client.publish "not_intercepted3" "another payload3".to-byte-array
        --qos=1
        --persistence-token="not_intercepted3"
    release-ack-packets.set true
    wait-for-idle.call
    activity /List := get-activity.call
    client.close

    // Check that no packet-id was reused and we have 4 different acks.
    expect-equals 4 ack-ids.size

    persistence-store-acked := persistence-store.acked
    expect-equals 5 persistence-store-acked.size
    persistence-store-acked.do: | packet/mqtt.PersistedPacket |
      expect-equals packet.topic packet.token

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda |
    test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
