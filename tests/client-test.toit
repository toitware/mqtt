// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import monitor
import mqtt.client show *
import mqtt.session-options show *
import mqtt.packets show *
import mqtt.errors show *
import mqtt.retry show *
import .support.peer

new-client connector/ScriptConnector --limit/int=4 -> Client:
  return Client --connector=connector
      --options=(SessionOptions --client-id="test" --keep-alive=Duration.ZERO --max-pending=limit)
      --retry=(RetryPolicy --initial-delay=Duration.ZERO --maximum-delay=Duration.ZERO --attempts=2)

publish-completes-on-ack:
  peer := Peer --fragmented
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := new-client connector
  client.start
  peer.accept
  client.wait-connected
  payload := #[1, 2, 3]
  receipt := client.publish "sensor" payload
  payload[0] = 99
  packet := peer.receive as PublishPacket
  expect-equals #[1, 2, 3] packet.payload
  expect-not receipt.is-complete
  peer.send (PubAckPacket --packet-id=packet.packet-id)
  receipt.wait
  client.close
  expect peer.receive is DisconnectPacket
  client.wait-closed

reconnect-replays-and-restores:
  first := Peer
  second := Peer
  connector := ScriptConnector
  connector.links.add first.client-link
  connector.links.add second.client-link
  client := new-client connector
  client.start
  first.accept
  client.wait-connected
  subscribed := client.subscribe "events/#"
  subscription := first.receive as SubscribePacket
  first.send (SubAckPacket --packet-id=subscription.packet-id --qos=[1])
  subscribed.wait
  receipt := client.publish "sensor" #[42]
  original := first.receive as PublishPacket
  first.close
  second.accept --no-session-present
  replay := second.receive as PublishPacket
  expect replay.duplicate
  expect-equals original.packet-id replay.packet-id
  expect-equals #[42] replay.payload
  restored := second.receive as SubscribePacket
  expect-equals "events/#" restored.topics.first.topic
  second.send (SubAckPacket --packet-id=restored.packet-id --qos=[1])
  second.send (PubAckPacket --packet-id=replay.packet-id)
  receipt.wait
  client.close --force
  client.wait-closed

close-wakes-producers-and-receivers:
  peer := Peer
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := new-client connector --limit=1
  client.start
  peer.accept
  client.wait-connected
  first := client.publish "sensor" #[1]
  peer.receive
  blocked := Completion_
  entered := monitor.Latch
  task::
    entered.set true
    failure := catch: client.publish "sensor" #[2]
    blocked.complete --failure=failure
  entered.get
  client.close --force
  client.wait-closed
  expect-not-null (catch: first.wait)
  expect-not-null (catch: blocked.wait)
  expect-null client.receive

wrong-ack-fails-every-observer:
  peer := Peer
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := new-client connector
  client.start
  peer.accept
  client.wait-connected
  receipt := client.publish "sensor" #[1]
  sent := peer.receive as PublishPacket
  peer.send (UnsubAckPacket --packet-id=sent.packet-id)
  failure := catch: client.wait-closed
  expect failure is ProtocolError
  expect-identical failure (catch: receipt.wait)
  expect-identical failure (catch: client.receive)
  expect-identical failure (catch: client.wait-closed)

subscription-rejection-is-local:
  peer := Peer
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := new-client connector
  client.start
  peer.accept
  client.wait-connected
  receipt := client.subscribe "forbidden"
  sent := peer.receive as SubscribePacket
  peer.send (SubAckPacket --packet-id=sent.packet-id --qos=[0x80])
  expect-not-null (catch: receipt.wait)
  still-alive := client.publish "sensor" #[] --qos=0
  expect peer.receive is PublishPacket
  still-alive.wait
  client.close --force
  client.wait-closed

partial-payload-is-not-delivered:
  first := Peer
  second := Peer
  connector := ScriptConnector
  connector.links.add first.client-link
  connector.links.add second.client-link
  client := new-client connector
  client.start
  first.accept
  client.wait-connected
  first.send-bytes #[0x32, 8, 0, 1, 'x', 0, 7, 42]
  first.close
  second.accept
  client.wait-connected
  // A complete message from the new connection is the first delivered message.
  second.send (PublishPacket "x" #[2] --qos=1 --retain=false --packet-id=8)
  message := client.receive
  expect-equals #[2] message.payload
  ack := second.receive as PubAckPacket
  expect-equals 8 ack.packet-id
  // The first connection never received an acknowledgement.
  expect-null first.receive
  client.close --force
  client.wait-closed

close-before-start:
  client := new-client ScriptConnector
  client.close
  client.wait-closed
  expect-null client.receive

main:
  with-timeout --ms=5_000:
    publish-completes-on-ack
    reconnect-replays-and-restores
    close-wakes-producers-and-receivers
    wrong-ack-fails-every-observer
    subscription-rejection-is-local
    partial-payload-is-not-delivered
    close-before-start
