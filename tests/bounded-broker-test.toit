// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import mqtt.bounded-broker show *
import mqtt.packets show *
import mqtt.last-will show *
import mqtt.topic-qos show *
import .support.peer

connect listener/MemoryListener id/string --clean/bool=false --will/LastWill?=null -> Peer:
  peer := Peer
  listener.add peer.client-link
  peer.send (ConnectPacket id --clean-session=clean --username=null --password=null
      --keep-alive=Duration.ZERO
      --last-will=will)
  expect peer.receive is ConnAckPacket
  return peer

subscribe peer/Peer filter/string:
  peer.send (SubscribePacket [TopicQos filter] --packet-id=1)
  ack := peer.receive as SubAckPacket
  expect-equals [1] ack.qos

/** Uses a ping as a barrier after the peer's previous packet was processed. */
barrier peer/Peer:
  peer.send PingReqPacket
  expect peer.receive is PingRespPacket

bounded-offline-delivery:
  listener := MemoryListener
  broker := Broker listener --limits=(BrokerLimits --queued-per-session=1 --payload-bytes=16)
  broker.start
  subscriber := connect listener "subscriber"
  subscribe subscriber "events/#"
  subscriber.send DisconnectPacket
  expect-null subscriber.receive
  publisher := connect listener "publisher" --clean
  publisher.send (PublishPacket "events/one" #[1, 2, 3] --qos=1 --retain=false --packet-id=1)
  expect publisher.receive is PubAckPacket
  expect-equals 1 broker.stats["deliveries"]
  expect-equals 3 broker.stats["payload-bytes"]

  // A full queue must not acknowledge a second message it cannot own.
  publisher.send (PublishPacket "events/two" #[4] --qos=1 --retain=false --packet-id=2)
  expect-null publisher.receive
  expect-equals 1 broker.stats["deliveries"]

  subscriber = connect listener "subscriber"
  message := subscriber.receive as PublishPacket
  expect-equals #[1, 2, 3] message.payload
  expect message.packet-id != 0
  subscriber.send (PubAckPacket --packet-id=message.packet-id)
  barrier subscriber
  expect-equals 0 broker.stats["payload-bytes"]
  broker.close
  broker.wait-closed

retained-filter-and-reference-counts:
  listener := MemoryListener
  broker := Broker listener
  broker.start
  broker.publish "a/b" #[42] --retain
  peer := connect listener "subscriber"
  // SUBACK and retained PUBLISH may arrive in either order.
  peer.send (SubscribePacket [TopicQos "a/#"] --packet-id=1)
  message/PublishPacket? := null
  2.repeat:
    packet := peer.receive
    if packet is PublishPacket: message = packet as PublishPacket
    else: expect packet is SubAckPacket
  expect-not-null message
  expect message.retain
  peer.send (PubAckPacket --packet-id=message.packet-id)
  barrier peer
  expect-equals 1 broker.stats["payload-bytes"]
  broker.publish "a/b" #[] --retain
  empty := peer.receive as PublishPacket
  peer.send (PubAckPacket --packet-id=empty.packet-id)
  barrier peer
  expect-equals 0 broker.stats["retained"]
  expect-equals 0 broker.stats["payload-bytes"]
  broker.close
  broker.wait-closed

malformed-client-does-not-stop-broker:
  listener := MemoryListener
  broker := Broker listener
  broker.start
  bad := connect listener "bad" --clean
  bad.send-bytes #[0xc1, 0]
  expect-null bad.receive
  good := connect listener "good" --clean
  barrier good
  expect-equals 1 broker.stats["client-errors"]
  broker.close
  broker.wait-closed

session-capacity-and-expiry:
  listener := MemoryListener
  broker := Broker listener --limits=(BrokerLimits --sessions=1)
  broker.start
  old := connect listener "old"
  old.send DisconnectPacket
  expect-null old.receive
  rejected := Peer
  listener.add rejected.client-link
  rejected.send (ConnectPacket "new" --clean-session=false --username=null --password=null
      --keep-alive=Duration.ZERO
      --last-will=null)
  expect-null rejected.receive
  expect-equals 1 broker.stats["sessions"]
  broker.expire-sessions --now=(Time.monotonic-us + (Duration --h=2).in-us)
  fresh := connect listener "new"
  barrier fresh
  broker.close
  broker.wait-closed

last-will-is-owned-and-released:
  listener := MemoryListener
  broker := Broker listener --limits=(BrokerLimits --payload-bytes=1)
  broker.start
  subscriber := connect listener "subscriber"
  subscribe subscriber "status"
  will := LastWill "status" #[9] --qos=1
  publisher := connect listener "publisher" --clean --will=will
  expect-equals 1 broker.stats["payload-bytes"]
  publisher.close
  message := subscriber.receive as PublishPacket
  expect-equals #[9] message.payload
  subscriber.send (PubAckPacket --packet-id=message.packet-id)
  barrier subscriber
  expect-equals 0 broker.stats["payload-bytes"]
  broker.close
  broker.wait-closed

connection-and-subscription-limits:
  listener := MemoryListener
  broker := Broker listener --limits=(BrokerLimits --connections=1 --subscriptions=1)
  broker.start
  peer := connect listener "owner" --clean
  subscribe peer "a/#"
  peer.send (SubscribePacket [TopicQos "b/#"] --packet-id=2)
  expect-equals [0x80] (peer.receive as SubAckPacket).qos
  rejected := Peer
  listener.add rejected.client-link
  expect-null rejected.receive
  broker.publish "a" #[7]
  publish := peer.receive as PublishPacket
  expect-equals #[7] publish.payload
  peer.send (PubAckPacket --packet-id=publish.packet-id)
  barrier peer
  broker.close
  broker.wait-closed

replacement-connection-replays-unacknowledged-data:
  listener := MemoryListener
  broker := Broker listener
  broker.start
  old := connect listener "same-id"
  subscribe old "a"
  broker.publish "a" #[7]
  original := old.receive as PublishPacket
  replacement := connect listener "same-id"
  expect-null old.receive
  replay := replacement.receive as PublishPacket
  expect replay.duplicate
  expect-equals original.packet-id replay.packet-id
  replacement.send (PubAckPacket --packet-id=replay.packet-id)
  barrier replacement
  expect-equals 0 broker.stats["payload-bytes"]
  broker.close
  broker.wait-closed

main:
  with-timeout --ms=5_000:
    bounded-offline-delivery
    retained-filter-and-reference-counts
    malformed-client-does-not-stop-broker
    session-capacity-and-expiry
    last-will-is-owned-and-released
    connection-and-subscription-limits
    replacement-connection-replays-unacknowledged-data
