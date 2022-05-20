// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import bytes
import binary
import reader
import .client show Client  // For toitdoc reference.

import .topic_filter

interface PacketIDAck:
  packet_id -> int

class LastWill:
  retain/bool
  qos/int
  topic/string
  payload/ByteArray

  /**
  Constructs the configuration of a last-will message.

  The parameters $topic, $payload, $qos and $retain have the same
    meaning as for $Client.publish, and are used when the last-will message
    is eventually sent.
  */
  constructor .topic .payload --.qos --.retain=false:
    if not 0 <= qos <= 2: throw "INVALID_QOS"

abstract class Packet:
  type/int
  flags/int

  constructor .type --.flags=0:

  static deserialize reader/reader.BufferedReader -> Packet:
    byte1 := reader.read_byte
    kind := byte1 >> 4
    flags := byte1 & 0xf
    size := 0
    for i := 0; i < 4; i++:
      byte := reader.read_byte
      size |= (byte & 0x7f) << (i * 7)
      if byte & 0x80 == 0: break
    if kind == ConnAckPacket.TYPE:
      return ConnAckPacket.deserialize reader
    if kind == PublishPacket.TYPE:
      return PublishPacket.deserialize reader size flags
    if kind == PubAckPacket.TYPE:
      return PubAckPacket.deserialize reader
    if kind == SubAckPacket.TYPE:
      return SubAckPacket.deserialize reader size
    if kind == PingRespPacket.TYPE:
      return PingRespPacket.deserialize reader

    throw "invalid packet kind: $kind"

  abstract variable_header -> ByteArray

  abstract payload -> ByteArray

  serialize -> ByteArray:
    buffer := bytes.Buffer
    buffer.put_byte type << 4 | flags

    header := variable_header
    payload := payload

    encode_length buffer header.size + payload.size

    buffer.write header
    buffer.write payload

    return buffer.bytes

  static encode_length buffer/bytes.Buffer length/int:
    if length == 0:
      buffer.put_byte 0
      return

    while length > 0:
      byte := length & 0x7f
      length >>= 7
      if length > 0: byte |= 0x80
      buffer.put_byte byte

  static encode_string buffer/bytes.Buffer str/string:
    length := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 length 0 str.size
    buffer.write length
    buffer.write str

class ConnectPacket extends Packet:
  static TYPE ::= 1

  client_id/string
  username/string?
  password/string?
  keep_alive/Duration?
  last_will/LastWill?

  constructor .client_id --.username=null --.password=null --.keep_alive=null --.last_will=null:
    super TYPE

  variable_header -> ByteArray:
    connect_flags := 0b0000_0010
    if username: connect_flags |= 0b1000_0000
    if password: connect_flags |= 0b0100_0000
    if last_will:
      connect_flags            |= 0b0000_0100
      connect_flags            |= last_will.qos << 3
      if last_will.retain:
        connect_flags          |= 0b0010_0000


    data := #[0, 4, 'M', 'Q', 'T', 'T', 4, connect_flags, 0, 0]
    binary.BIG_ENDIAN.put_uint16 data 8 keep_alive.in_s
    return data

  payload -> ByteArray:
    buffer := bytes.Buffer
    Packet.encode_string buffer client_id
    if last_will:
      Packet.encode_string buffer last_will.topic
      buffer.write #[last_will.payload.size >> 8, last_will.payload.size & 0xFF]
      buffer.write last_will.payload

    if username: Packet.encode_string buffer username
    if password: Packet.encode_string buffer password
    return buffer.bytes

class ConnAckPacket extends Packet:
  static TYPE ::= 2

  return_code/int

  constructor:
    return_code = 0
    super TYPE

  constructor.deserialize reader/reader.BufferedReader:
    data := reader.read_bytes 2
    return_code = data[1]
    super TYPE

  variable_header -> ByteArray:
    return #[0, 0]

  payload -> ByteArray: return #[]

class PublishPacket extends Packet:
  static TYPE ::= 3

  topic/string
  payload/ByteArray
  qos/int
  packet_id/int?

  constructor.deserialize reader/reader.BufferedReader size/int flags/int:
    qos = (flags >> 1) & 0b11
    data := reader.read_bytes 2
    topic_length := binary.BIG_ENDIAN.uint16 data 0
    topic = reader.read_string topic_length
    size -= 2 + topic_length
    if qos > 0:
      data = reader.read_bytes 2
      packet_id = binary.BIG_ENDIAN.uint16 data 0
      size -= 2
    else:
      packet_id = null
    payload = reader.read_bytes size
    super TYPE

  constructor .topic .payload --.qos/int --retain --.packet_id:
    super TYPE
      --flags=(qos << 1) | (retain ? 1 : 0)

  variable_header -> ByteArray:
    buffer := bytes.Buffer
    Packet.encode_string buffer topic
    if packet_id:
      data := ByteArray 2
      binary.BIG_ENDIAN.put_uint16 data 0 packet_id
      buffer.write data
    return buffer.bytes

class PubAckPacket extends Packet implements PacketIDAck:
  static TYPE ::= 4

  packet_id/int

  constructor .packet_id:
    super TYPE

  constructor.deserialize reader/reader.BufferedReader:
    data := reader.read_bytes 2
    packet_id = binary.BIG_ENDIAN.uint16 data 0
    super TYPE

  variable_header -> ByteArray:
    data := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 data 0 packet_id
    return data

  payload -> ByteArray: return #[]

class SubscribePacket extends Packet:
  static TYPE ::= 8

  topic_filters/List/*<TopicFilter>*/
  packet_id/int

  constructor .topic_filters --.packet_id:
    super TYPE --flags=0b0010

  variable_header -> ByteArray:
    data := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 data 0 packet_id
    return data

  payload -> ByteArray:
    buffer := bytes.Buffer
    topic_filters.do: | topic_filter/TopicFilter |
      Packet.encode_string buffer topic_filter.filter
      buffer.put_byte topic_filter.max_qos
    return buffer.bytes

class SubAckPacket extends Packet implements PacketIDAck:
  static TYPE ::= 9

  /** The qos value for a failed subscription. */
  static FAILED_SUBSCRIPTION_QOS ::= 0x80

  packet_id /int
  qos /List  // The list of qos matches the list of topics from the SubPacket.

  constructor .packet_id --.qos:
    super TYPE

  constructor.deserialize reader/reader.BufferedReader size/int:
    data := reader.read_bytes 2
    size -= 2
    packet_id = binary.BIG_ENDIAN.uint16 data 0
    qos = List size: reader.read_byte
    super TYPE

  variable_header -> ByteArray:
    data := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 data 0 packet_id
    return data

  payload -> ByteArray: return #[]

class UnsubscribePacket extends Packet:
  static TYPE ::= 10

  topic_filters/List/*<string>*/
  packet_id/int

  constructor .topic_filters --.packet_id:
    super TYPE --flags=0b0010

  variable_header -> ByteArray:
    data := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 data 0 packet_id
    return data

  payload -> ByteArray:
    buffer := bytes.Buffer
    topic_filters.do: | topic_filter/string |
      Packet.encode_string buffer topic_filter
    return buffer.bytes

class UnsubAckPacket extends Packet implements PacketIDAck:
  static TYPE ::= 11

  packet_id/int

  constructor .packet_id:
    super TYPE

  constructor.deserialize reader/reader.BufferedReader:
    data := reader.read_bytes 2
    packet_id = binary.BIG_ENDIAN.uint16 data 0
    super TYPE

  variable_header -> ByteArray:
    data := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 data 0 packet_id
    return data

  payload -> ByteArray: return #[]

class PingReqPacket extends Packet:
  static TYPE ::= 12

  constructor:
    super TYPE

  variable_header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]

class PingRespPacket extends Packet:
  static TYPE ::= 13

  constructor:
    super TYPE

  constructor.deserialize reader/reader.BufferedReader:
    super TYPE

  variable_header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]

class DisconnectPacket extends Packet:
  static TYPE ::= 14

  constructor:
    super TYPE

  variable_header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]
