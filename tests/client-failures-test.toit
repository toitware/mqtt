// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import mqtt
import mqtt.completion show Completion_
import mqtt.packets show *
import .support.peer

class DelayedWrite implements mqtt.Link:
  wrapped/mqtt.Link
  sent := Completion_
  release := Completion_
  fail/bool
  constructor .wrapped --.fail=false:
  read -> ByteArray?: return wrapped.read
  write bytes/ByteArray -> int:
    count := wrapped.write bytes
    if bytes[0] >> 4 == PublishPacket.TYPE:
      sent.complete
      if fail: throw "RESET_AFTER_WRITE"
      release.wait
    return count
  close -> none:
    release.complete
    wrapped.close

class Offline implements mqtt.Connector:
  attempted := Completion_
  count := 0
  open -> mqtt.Link:
    count++
    attempted.complete
    throw "OFFLINE"

new-client connector/mqtt.Connector --retry/mqtt.RetryPolicy?=null -> mqtt.Client:
  retry = retry or mqtt.RetryPolicy --attempts=2
      --initial-delay=Duration.ZERO
      --maximum-delay=Duration.ZERO
  return mqtt.Client --connector=connector --retry=retry
      --options=(mqtt.SessionOptions --client-id="fault-test" --keep-alive=Duration.ZERO)

ack-before-write-returns:
  peer := Peer
  link := DelayedWrite peer.client-link
  connector := ScriptConnector
  connector.links.add link
  client := new-client connector
  client.start
  peer.accept
  client.wait-connected
  receipt := client.publish "x" #[1]
  packet := peer.receive as PublishPacket
  link.sent.wait
  peer.send (PubAckPacket --packet-id=packet.packet-id)
  expect-not receipt.is-complete
  link.release.complete
  receipt.wait
  client.close --force
  client.wait-closed

ambiguous-write-is-replayed:
  first := Peer
  second := Peer
  connector := ScriptConnector
  connector.links.add (DelayedWrite first.client-link --fail)
  connector.links.add second.client-link
  client := new-client connector
  client.start
  first.accept
  client.wait-connected
  receipt := client.publish "x" #[1]
  original := first.receive as PublishPacket
  second.accept --session-present
  replay := second.receive as PublishPacket
  expect replay.duplicate
  expect-equals original.packet-id replay.packet-id
  second.send (PubAckPacket --packet-id=replay.packet-id)
  receipt.wait
  client.close --force
  client.wait-closed

close-interrupts-backoff:
  connector := Offline
  client := new-client connector --retry=(mqtt.RetryPolicy --initial-delay=(Duration --h=1)
      --maximum-delay=(Duration --h=1))
  client.start
  connector.attempted.wait
  client.close
  client.wait-closed
  expect-equals 1 connector.count

retry-exhaustion-fails-queued-work:
  connector := Offline
  client := new-client connector
  client.start
  receipt := client.publish "x" #[1]
  failure := catch: client.wait-closed
  expect failure is mqtt.ConnectionError
  expect-equals "failed" client.connection-state
  expect-identical failure client.last-connection-error
  expect-equals 3 connector.count
  expect-identical failure (catch: receipt.wait)
  expect-identical failure (catch: client.receive)

authentication-is-not-retried:
  peer := Peer
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := new-client connector
  client.start
  expect peer.receive is ConnectPacket
  peer.send (ConnAckPacket --return-code=4)
  failure := catch: client.wait-closed
  expect failure is mqtt.MqttError
  expect-equals "CONNECTION_REFUSED" failure.kind
  expect-equals 1 connector.attempts

inbox-overflow-is-observable:
  peer := Peer
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := mqtt.Client --connector=connector --max-incoming=1
      --options=(mqtt.SessionOptions --client-id="bounded" --keep-alive=Duration.ZERO)
  client.start
  peer.accept
  client.wait-connected
  peer.send (PublishPacket "x" #[1] --qos=1 --retain=false --packet-id=1)
  expect peer.receive is PubAckPacket
  peer.send (PublishPacket "x" #[2] --qos=1 --retain=false --packet-id=2)
  failure := catch: client.wait-closed
  expect failure is mqtt.CapacityError
  expect-equals #[1] client.receive.payload
  expect-identical failure (catch: client.receive)
  expect-null peer.receive

missing-pong-fails-the-connection:
  peer := Peer
  connector := ScriptConnector
  connector.links.add peer.client-link
  client := mqtt.Client --connector=connector --retry=(mqtt.RetryPolicy --attempts=0)
      --options=(mqtt.SessionOptions --client-id="ping" --keep-alive=(Duration --s=1))
  client.start
  peer.accept
  client.wait-connected
  expect peer.receive is PingReqPacket
  // Deliberately omit PINGRESP. The real scheduler must run the deadline path.
  failure := catch: client.wait-closed
  expect failure is mqtt.ConnectionError
  expect-equals "PING_TIMEOUT" failure.kind

main:
  with-timeout --ms=5_000:
    ack-before-write-returns
    ambiguous-write-is-replayed
    close-interrupts-backoff
    retry-exhaustion-fails-queued-work
    authentication-is-not-retried
    inbox-overflow-is-observable
    missing-pong-fails-the-connection
