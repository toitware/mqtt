// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import io

import mqtt.packets as mqtt
import mqtt.last-will as mqtt
import mqtt.topic-qos as mqtt

PACKET-ID-TESTS ::= [
    0,
    1,
    2,
    50_000,
]

test-roundtrip packet/mqtt.Packet -> mqtt.Packet:
  serialized := packet.serialize
  reader := io.Reader serialized
  deserialized := mqtt.Packet.deserialize reader
  serialized2 := deserialized.serialize
  expect-equals serialized serialized2
  return deserialized

test-connect:
  TESTS ::= [
    {
      "client-id": "simple",
      "clean-session": true,
      "keep-alive": Duration --s=5,
    },
    {
      // The specification only requires 0-9a-zA-Z, but brokers are allowed to accept more.
      "client-id": "username-password",
      "clean-session": true,
      "username": "username",
      "password": "password",
      "keep-alive": Duration --s=5,
    },
    {
      // The specification requires that client-ids can be at least 23 bytes long.
      // It allows for longer ids.
      "client-id": "long_id-10" * 10,
      "clean-session": false,
      "username": "long_username" * 10,
      "password": "long_password" * 10,
      "keep-alive": Duration --s=63_000, // 16 bits.
    },
    {
      "client-id": "last_will",
      "clean-session": true,
      "username": "username",
      "keep-alive": Duration --s=0,
      "last-will": {
        "topic": "last_will_topic",
        "payload": "last_will_payload".to-byte-array,
        "retain": false,
        "qos": 0,
      },
    },
    {
      "client-id": "last_will2",
      "clean-session": false,
      "password": "password",
      "keep-alive": Duration --s=60,
      "last-will": {
        "topic": "last_will_topic" * 10,
        "payload": ("last_will_payload" * 10).to-byte-array,
        "retain": true,
        "qos": 1,
      },
    },
    {
      "client-id": "last_will-huge-payload",
      "clean-session": false,
      "password": "password",
      "keep-alive": Duration --s=60,
      "last-will": {
        "topic": "last_will_topic" * 10,
        "payload": ("last_will_payload" * 1000).to-byte-array,
        "retain": true,
        "qos": 1,
      },
    },
  ]

  saw-last-will := false
  TESTS.do: | test |
    client-id := test["client-id"]
    clean-session := test["clean-session"]
    username := test.get "username"
    password := test.get "password"
    keep-alive := test["keep-alive"]
    last-will-entry := test.get "last-will"
    last-will /mqtt.LastWill? := null
    if last-will-entry:
      saw-last-will = true
      last-will-topic := last-will-entry["topic"]
      last-will-payload := last-will-entry["payload"]
      last-will-retain := last-will-entry["retain"]
      last-will-qos := last-will-entry["qos"]
      last-will = mqtt.LastWill last-will-topic last-will-payload --qos=last-will-qos --retain=last-will-retain

    connect := mqtt.ConnectPacket client-id
        --clean-session=clean-session
        --username=username
        --password=password
        --keep-alive=keep-alive
        --last-will=last-will

    deserialized := (test-roundtrip connect) as mqtt.ConnectPacket
    expect-equals client-id deserialized.client-id
    expect-equals clean-session deserialized.clean-session
    expect-equals username deserialized.username
    expect-equals password deserialized.password
    expect-equals keep-alive deserialized.keep-alive
    if last-will-entry:
      deserialized-last-will := deserialized.last-will
      expect-equals last-will-entry["topic"] deserialized-last-will.topic
      expect-equals last-will-entry["payload"] deserialized-last-will.payload
      expect-equals last-will-entry["retain"] deserialized-last-will.retain
      expect-equals last-will-entry["qos"] deserialized-last-will.qos
    else:
      expect-null deserialized.last-will

  expect: saw-last-will

test-connack:
  TESTS ::= [
    0,
    mqtt.ConnAckPacket.UNACCEPTABLE-PROTOCOL-VERSION,
    mqtt.ConnAckPacket.IDENTIFIER-REJECTED,
    mqtt.ConnAckPacket.SERVER-UNAVAILABLE,
    mqtt.ConnAckPacket.BAD-USERNAME-OR-PASSWORD,
    mqtt.ConnAckPacket.NOT-AUTHORIZED,
  ]

  TESTS.do: | test-code |
    2.repeat:
      session-present := it == 0
      // The specification requires that the session_present is false when there is a
      // return-code that is different from 0.
      // We still test all combinations.
      connack := mqtt.ConnAckPacket --return-code=test-code --session-present=session-present
      deserialized := (test-roundtrip connack) as mqtt.ConnAckPacket
      expect-equals test-code deserialized.return-code
      expect-equals session-present deserialized.session-present

test-publish:
  TESTS ::= [
    {
      "topic": "topic",
      "payload": "payload".to-byte-array,
      "qos": 0,
      "retain": false,
      "packet-id": null,
      "duplicate": false,
    },
    {
      "topic": "topic",
      "payload": ("huge payload" * 1000).to-byte-array,
      "qos": 1,
      "retain": true,
      "packet-id": 50_000,
      "duplicate": true,
    },
    {
      "topic": "foo/bar/gee",
      "payload": "bar".to-byte-array,
      "qos": 0,
      "retain": false,
      "packet-id": null,
      "duplicate": false,
    },
  ]

  TESTS.do: | test |
    topic := test["topic"]
    payload := test["payload"]
    qos := test["qos"]
    retain := test["retain"]
    packet-id := test["packet-id"]
    duplicate := test["duplicate"]
    publish := mqtt.PublishPacket topic payload
        --qos = qos
        --retain = retain
        --packet-id = packet-id
        --duplicate = duplicate

    deserialized := (test-roundtrip publish) as mqtt.PublishPacket
    expect-equals topic deserialized.topic
    expect-equals payload.size deserialized.payload.size
    expect-equals payload deserialized.payload
    expect-equals qos deserialized.qos
    expect-equals retain deserialized.retain
    expect-equals packet-id deserialized.packet-id
    expect-equals duplicate deserialized.duplicate

test-puback:
  PACKET-ID-TESTS.do: | packet-id |
    puback := mqtt.PubAckPacket --packet-id=packet-id
    deserialized := (test-roundtrip puback) as mqtt.PubAckPacket
    expect-equals packet-id deserialized.packet-id

test-subscribe:
  TESTS ::= [
    {
      "packet-id": 0,
      "filters": [
        [ "topic1", 0 ],
      ]
    },
    {
      "packet-id": 1000,
      "filters": [
        [ "topic1", 0 ],
        [ "topic2", 1 ],
      ],
    },
    {
      "packet-id": 50000,
      "filters": [
        [ "complicated/" * 300 + "end", 0 ],
        [ "+" * 20, 1 ],
      ],
    },
    {
      "packet-id": 499,
      "filters": List 500: [ "topic $it", 1 ],
    },
  ]

  TESTS.do: | test |
    packet-id := test["packet-id"]
    topic-qos-values := test["filters"]
    topic-qoses := topic-qos-values.map: | values |
      topic := values[0]
      max-qos := values[1]
      mqtt.TopicQos topic --max-qos=max-qos
    subscribe := mqtt.SubscribePacket --packet-id=packet-id topic-qoses
    deserialized := (test-roundtrip subscribe) as mqtt.SubscribePacket
    expect-equals packet-id deserialized.packet-id
    expect-equals topic-qos-values.size deserialized.topics.size
    topic-qos-values.size.repeat:
      values := topic-qos-values[it]
      topic-qos := deserialized.topics[it]
      expect-equals values[0] topic-qos.topic
      expect-equals values[1] topic-qos.max-qos

test-suback:
  TESTS ::= [
    {
      "packet-id": 0,
      "qos-list": [ 0 ],
    },
    {
      "packet-id": 1000,
      "qos-list": [ 0, 1 ],
    },
    {
      "packet-id": 50000,
      "qos-list": List 500: it % 2,
    },
  ]

  TESTS.do: | test |
    packet-id := test["packet-id"]
    qos-list := test["qos-list"]
    suback := mqtt.SubAckPacket --packet-id=packet-id --qos=qos-list
    deserialized := (test-roundtrip suback) as mqtt.SubAckPacket
    expect-equals packet-id deserialized.packet-id
    expect-equals qos-list.size deserialized.qos.size
    expect-equals qos-list deserialized.qos

test-unsubscribe:
  TESTS ::= [
    {
      "packet-id": 0,
      "filters": [ "topic1" ],
    },
    {
      "packet-id": 1000,
      "filters": [ "topic1", "topic2" ],
    },
    {
      "packet-id": 50000,
      "filters": [ "complicated/" * 300 + "end" ] + [ "+" * 20 ],
    },
    {
      "packet-id": 499,
      "filters": List 500: "topic $it",
    },
  ]

  TESTS.do: | test |
    packet-id := test["packet-id"]
    topics := test["filters"]
    unsubscribe := mqtt.UnsubscribePacket --packet-id=packet-id topics
    deserialized := (test-roundtrip unsubscribe) as mqtt.UnsubscribePacket
    expect-equals packet-id deserialized.packet-id
    expect-equals topics.size deserialized.topics.size
    expect-equals topics deserialized.topics

test-unsuback:
  PACKET-ID-TESTS.do: | packet-id |
    unsuback := mqtt.UnsubAckPacket --packet-id=packet-id
    deserialized := (test-roundtrip unsuback) as mqtt.UnsubAckPacket
    expect-equals packet-id deserialized.packet-id

test-pingreq:
  pingreq := mqtt.PingReqPacket
  deserialized := (test-roundtrip pingreq) as mqtt.PingReqPacket

test-pingresp:
  pingresp := mqtt.PingRespPacket
  deserialized := (test-roundtrip pingresp) as mqtt.PingRespPacket

test-disconnect:
  disconnect := mqtt.DisconnectPacket
  deserialized := (test-roundtrip disconnect) as mqtt.DisconnectPacket

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  if test-with-mosquitto: return

  test-connect
  test-connack
  test-publish
  test-puback
  test-subscribe
  test-suback
  test-unsubscribe
  test-unsuback
  test-pingreq
  test-pingresp
  test-disconnect
