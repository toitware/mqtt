// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import bytes
import expect show *
import reader

import mqtt.packets as mqtt
import mqtt.last_will as mqtt
import mqtt.topic_filter as mqtt

PACKET_ID_TESTS ::= [
    0,
    1,
    2,
    50_000,
]

test_roundtrip packet/mqtt.Packet -> mqtt.Packet:
  serialized := packet.serialize
  reader := reader.BufferedReader (bytes.Reader serialized)
  deserialized := mqtt.Packet.deserialize reader
  serialized2 := deserialized.serialize
  expect_equals serialized serialized2
  return deserialized

test_connect:
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
        "payload": "last_will_payload".to_byte_array,
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
        "payload": ("last_will_payload" * 10).to_byte_array,
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
        "payload": ("last_will_payload" * 1000).to_byte_array,
        "retain": true,
        "qos": 1,
      },
    },
  ]

  saw_last_will := false
  TESTS.do: | test |
    client_id := test["client-id"]
    clean_session := test["clean-session"]
    username := test.get "username"
    password := test.get "password"
    keep_alive := test["keep-alive"]
    last_will_entry := test.get "last-will"
    last_will /mqtt.LastWill? := null
    if last_will_entry:
      saw_last_will = true
      last_will_topic := last_will_entry["topic"]
      last_will_payload := last_will_entry["payload"]
      last_will_retain := last_will_entry["retain"]
      last_will_qos := last_will_entry["qos"]
      last_will = mqtt.LastWill last_will_topic last_will_payload --qos=last_will_qos --retain=last_will_retain

    connect := mqtt.ConnectPacket client_id
        --clean_session=clean_session
        --username=username
        --password=password
        --keep_alive=keep_alive
        --last_will=last_will

    deserialized := (test_roundtrip connect) as mqtt.ConnectPacket
    expect_equals client_id deserialized.client_id
    expect_equals clean_session deserialized.clean_session
    expect_equals username deserialized.username
    expect_equals password deserialized.password
    expect_equals keep_alive deserialized.keep_alive
    if last_will_entry:
      deserialized_last_will := deserialized.last_will
      expect_equals last_will_entry["topic"] deserialized_last_will.topic
      expect_equals last_will_entry["payload"] deserialized_last_will.payload
      expect_equals last_will_entry["retain"] deserialized_last_will.retain
      expect_equals last_will_entry["qos"] deserialized_last_will.qos
    else:
      expect_null deserialized.last_will

  expect: saw_last_will

test_connack:
  TESTS ::= [
    0,
    mqtt.ConnAckPacket.UNACCEPTABLE_PROTOCOL_VERSION,
    mqtt.ConnAckPacket.IDENTIFIER_REJECTED,
    mqtt.ConnAckPacket.SERVER_UNAVAILABLE,
    mqtt.ConnAckPacket.BAD_USERNAME_OR_PASSWORD,
    mqtt.ConnAckPacket.NOT_AUTHORIZED,
  ]

  TESTS.do: | test_code |
    2.repeat:
      session_present := it == 0
      // The specification requires that the session_present is false when there is a
      // return-code that is different from 0.
      // We still test all combinations.
      connack := mqtt.ConnAckPacket --return_code=test_code --session_present=session_present
      deserialized := (test_roundtrip connack) as mqtt.ConnAckPacket
      expect_equals test_code deserialized.return_code
      expect_equals session_present deserialized.session_present

test_publish:
  TESTS ::= [
    {
      "topic": "topic",
      "payload": "payload".to_byte_array,
      "qos": 0,
      "retain": false,
      "packet-id": null,
      "duplicate": false,
    },
    {
      "topic": "topic",
      "payload": ("huge payload" * 1000).to_byte_array,
      "qos": 1,
      "retain": true,
      "packet-id": 50_000,
      "duplicate": true,
    },
    {
      "topic": "foo/bar/gee",
      "payload": "bar".to_byte_array,
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
    packet_id := test["packet-id"]
    duplicate := test["duplicate"]
    publish := mqtt.PublishPacket topic payload
        --qos = qos
        --retain = retain
        --packet_id = packet_id
        --duplicate = duplicate

    deserialized := (test_roundtrip publish) as mqtt.PublishPacket
    expect_equals topic deserialized.topic
    expect_equals payload.size deserialized.payload.size
    expect_equals payload deserialized.payload
    expect_equals qos deserialized.qos
    expect_equals retain deserialized.retain
    expect_equals packet_id deserialized.packet_id
    expect_equals duplicate deserialized.duplicate

test_puback:
  PACKET_ID_TESTS.do: | packet_id |
    puback := mqtt.PubAckPacket packet_id
    deserialized := (test_roundtrip puback) as mqtt.PubAckPacket
    expect_equals packet_id deserialized.packet_id

test_subscribe:
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
    packet_id := test["packet-id"]
    topic_filter_values := test["filters"]
    topic_filters := topic_filter_values.map: | values |
      filter := values[0]
      max_qos := values[1]
      mqtt.TopicFilter filter --max_qos=max_qos
    subscribe := mqtt.SubscribePacket --packet_id=packet_id topic_filters
    deserialized := (test_roundtrip subscribe) as mqtt.SubscribePacket
    expect_equals packet_id deserialized.packet_id
    expect_equals topic_filter_values.size deserialized.topic_filters.size
    topic_filter_values.size.repeat:
      values := topic_filter_values[it]
      topic_filter := deserialized.topic_filters[it]
      expect_equals values[0] topic_filter.filter
      expect_equals values[1] topic_filter.max_qos

test_suback:
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
    packet_id := test["packet-id"]
    qos_list := test["qos-list"]
    suback := mqtt.SubAckPacket --packet_id=packet_id --qos=qos_list
    deserialized := (test_roundtrip suback) as mqtt.SubAckPacket
    expect_equals packet_id deserialized.packet_id
    expect_equals qos_list.size deserialized.qos.size
    expect_equals qos_list deserialized.qos

test_unsubscribe:
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
    packet_id := test["packet-id"]
    topic_filters := test["filters"]
    unsubscribe := mqtt.UnsubscribePacket --packet_id=packet_id topic_filters
    deserialized := (test_roundtrip unsubscribe) as mqtt.UnsubscribePacket
    expect_equals packet_id deserialized.packet_id
    expect_equals topic_filters.size deserialized.topic_filters.size
    expect_equals topic_filters deserialized.topic_filters

test_unsuback:
  PACKET_ID_TESTS.do: | packet_id |
    unsuback := mqtt.UnsubAckPacket packet_id
    deserialized := (test_roundtrip unsuback) as mqtt.UnsubAckPacket
    expect_equals packet_id deserialized.packet_id

test_pingreq:
  pingreq := mqtt.PingReqPacket
  deserialized := (test_roundtrip pingreq) as mqtt.PingReqPacket

test_pingresp:
  pingresp := mqtt.PingRespPacket
  deserialized := (test_roundtrip pingresp) as mqtt.PingRespPacket

test_disconnect:
  disconnect := mqtt.DisconnectPacket
  deserialized := (test_roundtrip disconnect) as mqtt.DisconnectPacket

main:
  test_connect
  test_connack
  test_publish
  test_puback
  test_subscribe
  test_suback
  test_unsubscribe
  test_unsuback
  test_pingreq
  test_pingresp
  test_disconnect
