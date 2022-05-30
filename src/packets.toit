// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import bytes
import binary
import reader

import .last_will
import .topic_filter

abstract class Packet:
  type/int
  flags/int

  constructor .type --.flags=0:

  static deserialize reader/reader.BufferedReader -> Packet?:
    if not reader.can_ensure 1: return null
    byte1 := reader.read_byte
    kind := byte1 >> 4
    flags := byte1 & 0xf
    size := 0
    for i := 0; i < 4; i++:
      byte := reader.read_byte
      size |= (byte & 0x7f) << (i * 7)
      if byte & 0x80 == 0: break
    if kind == ConnectPacket.TYPE:
      return ConnectPacket.deserialize_ reader
    if kind == ConnAckPacket.TYPE:
      return ConnAckPacket.deserialize_ reader
    if kind == PublishPacket.TYPE:
      return PublishPacket.deserialize_ reader size flags
    if kind == PubAckPacket.TYPE:
      return PubAckPacket.deserialize_ reader
    if kind == SubscribePacket.TYPE:
      return SubscribePacket.deserialize_ reader size
    if kind == SubAckPacket.TYPE:
      return SubAckPacket.deserialize_ reader size
    if kind == UnsubscribePacket.TYPE:
      return UnsubscribePacket.deserialize_ reader size
    if kind == UnsubAckPacket.TYPE:
      return UnsubAckPacket.deserialize_ reader
    if kind == PingRespPacket.TYPE:
      return PingRespPacket.deserialize_ reader
    if kind == PingReqPacket.TYPE:
      return PingReqPacket.deserialize_ reader
    if kind == DisconnectPacket.TYPE:
      return DisconnectPacket.deserialize_ reader

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

  static encode_uint16 value/int -> ByteArray:
    result := ByteArray 2
    binary.BIG_ENDIAN.put_uint16 result 0 value
    return result

  static decode_string reader/reader.BufferedReader -> string:
    length := decode_uint16 reader
    return reader.read_string length

  static decode_uint16 reader/reader.BufferedReader -> int:
    length_bytes := reader.read_bytes 2
    return binary.BIG_ENDIAN.uint16 length_bytes 0

class ConnectPacket extends Packet:
  static TYPE ::= 1

  client_id /string
  clean_session /bool
  username /string?
  password /string?
  keep_alive /Duration?
  last_will /LastWill?

  constructor .client_id --.clean_session --.username --.password --.keep_alive --.last_will:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    protocol := Packet.decode_string reader
    if protocol != "MQTT": throw "UNEQUAL PROTOCOL $protocol"
    level := reader.read_byte
    if level != 4: throw "UNEXPECTED PROTOCOL LEVEL: $level"
    connect_flags := reader.read_byte
    clean_session = connect_flags & 0b0000_0010 != 0
    has_username := connect_flags & 0b1000_0000 != 0
    has_password := connect_flags & 0b0100_0000 != 0
    has_last_will := connect_flags & 0b0000_0100 != 0
    last_will_remain := connect_flags & 0b0010_0000 != 0
    last_will_qos := (connect_flags >> 3) & 0b11

    keep_alive = Duration --s=(Packet.decode_uint16 reader)

    client_id = Packet.decode_string reader
    if has_last_will:
      last_will_topic := Packet.decode_string reader
      last_will_payload_size := Packet.decode_uint16 reader
      last_will_payload := reader.read_bytes last_will_payload_size
      last_will = LastWill last_will_topic last_will_payload --qos=last_will_qos --retain=last_will_remain
    else:
      last_will = null

    username = has_username ? Packet.decode_string reader : null
    password = has_password ? Packet.decode_string reader : null

    super TYPE

  variable_header -> ByteArray:
    connect_flags := 0
    if clean_session: connect_flags |= 0b0000_0010
    if username:      connect_flags |= 0b1000_0000
    if password:      connect_flags |= 0b0100_0000
    if last_will:
      connect_flags                 |= 0b0000_0100
      connect_flags                 |= last_will.qos << 3
      if last_will.retain:
        connect_flags               |= 0b0010_0000


    data := #[0, 4, 'M', 'Q', 'T', 'T', 4, connect_flags, 0, 0]
    binary.BIG_ENDIAN.put_uint16 data 8 keep_alive.in_s
    return data

  payload -> ByteArray:
    buffer := bytes.Buffer
    Packet.encode_string buffer client_id
    if last_will:
      Packet.encode_string buffer last_will.topic
      buffer.write (Packet.encode_uint16 last_will.payload.size)
      buffer.write last_will.payload

    if username: Packet.encode_string buffer username
    if password: Packet.encode_string buffer password
    return buffer.bytes

class ConnAckPacket extends Packet:
  static TYPE ::= 2

  static UNACCEPTABLE_PROTOCOL_VERSION ::= 0x01
  static IDENTIFIER_REJECTED ::= 0x02
  static SERVER_UNAVAILABLE ::= 0x03
  static BAD_USERNAME_OR_PASSWORD ::= 0x04
  static NOT_AUTHORIZED ::= 0x05

  return_code /int
  session_present /bool

  constructor --.return_code=0 --.session_present=false:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    data := reader.read_bytes 2
    session_present = data[0] & 0x01 != 0
    return_code = data[1]
    super TYPE

  variable_header -> ByteArray:
    return #[ session_present ? 1 : 0, return_code]

  payload -> ByteArray: return #[]

class PublishPacket extends Packet:
  static TYPE ::= 3

  topic /string
  payload /ByteArray
  packet_id /int?

  constructor.deserialize_ reader/reader.BufferedReader size/int flags/int:
    retain := flags & 0b1 != 0
    qos := (flags >> 1) & 0b11
    duplicate := flags & 0b1000 != 0
    topic = Packet.decode_string reader
    size -= 2 + topic.size
    if qos > 0:
      packet_id = Packet.decode_uint16 reader
      size -= 2
    else:
      packet_id = null
    payload = reader.read_bytes size
    super TYPE --flags=(qos << 1) | (retain ? 1 : 0) | (duplicate ? 0b1000 : 0)

  constructor .topic .payload --qos/int --retain/bool --.packet_id --duplicate=false:
    super TYPE
        --flags=(qos << 1) | (retain ? 1 : 0) | (duplicate ? 0b1000 : 0)

  variable_header -> ByteArray:
    buffer := bytes.Buffer
    Packet.encode_string buffer topic
    if packet_id: buffer.write (Packet.encode_uint16 packet_id)
    return buffer.bytes

  retain -> bool: return flags & 0b1 != 0
  qos -> int: return (flags >> 1) & 0b11
  duplicate -> bool: return flags & 0b1000 != 0

  with --topic/string?=null --payload/ByteArray?=null --qos/int?=null --retain/bool?=null --packet_id/int?=null --duplicate/bool?=null:
    return PublishPacket
        topic or this.topic
        payload or this.payload
        --qos = qos or this.qos
        --retain = retain != null ? retain : this.retain
        --packet_id = packet_id or this.packet_id
        --duplicate = duplicate != null ? duplicate : this.duplicate

class PubAckPacket extends Packet:
  static TYPE ::= 4

  packet_id/int

  constructor .packet_id:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    packet_id = Packet.decode_uint16 reader
    super TYPE

  variable_header -> ByteArray:
    return Packet.encode_uint16 packet_id

  payload -> ByteArray: return #[]

class SubscribePacket extends Packet:
  static TYPE ::= 8

  topic_filters/List/*<TopicFilter>*/
  packet_id/int

  constructor .topic_filters --.packet_id:
    super TYPE --flags=0b0010

  constructor.deserialize_ reader/reader.BufferedReader size/int:
    packet_id = Packet.decode_uint16 reader
    size -= 2
    topic_filters = []
    while size > 0:
      topic := Packet.decode_string reader
      size -= 2 + topic.size
      max_qos := reader.read_byte
      size--
      topic_filter := TopicFilter topic --max_qos=max_qos
      topic_filters.add topic_filter
    super TYPE --flags=0b0010

  variable_header -> ByteArray:
    return Packet.encode_uint16 packet_id

  payload -> ByteArray:
    buffer := bytes.Buffer
    topic_filters.do: | topic_filter/TopicFilter |
      Packet.encode_string buffer topic_filter.filter
      buffer.put_byte topic_filter.max_qos
    return buffer.bytes

class SubAckPacket extends Packet:
  static TYPE ::= 9

  /** The qos value for a failed subscription. */
  static FAILED_SUBSCRIPTION_QOS ::= 0x80

  packet_id /int
  qos /List  // The list of qos matches the list of topics from the SubPacket.

  constructor --.packet_id --.qos:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader size/int:
    packet_id = Packet.decode_uint16 reader
    size -= 2
    qos = List size: reader.read_byte
    super TYPE

  variable_header -> ByteArray:
    return Packet.encode_uint16 packet_id

  payload -> ByteArray: return ByteArray qos.size: qos[it]

class UnsubscribePacket extends Packet:
  static TYPE ::= 10

  topic_filters/List/*<string>*/
  packet_id/int

  constructor.deserialize_ reader/reader.BufferedReader size/int:
    packet_id = Packet.decode_uint16 reader
    size -= 2
    topic_filters = []
    while size > 0:
      topic := Packet.decode_string reader
      size -= 2 + topic.size
      topic_filters.add topic
    super TYPE --flags=0b0010

  constructor .topic_filters --.packet_id:
    super TYPE --flags=0b0010

  variable_header -> ByteArray:
    return Packet.encode_uint16 packet_id

  payload -> ByteArray:
    buffer := bytes.Buffer
    topic_filters.do: | topic_filter/string |
      Packet.encode_string buffer topic_filter
    return buffer.bytes

class UnsubAckPacket extends Packet:
  static TYPE ::= 11

  packet_id /int

  constructor .packet_id:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    packet_id = Packet.decode_uint16 reader
    super TYPE

  variable_header -> ByteArray:
    return Packet.encode_uint16 packet_id

  payload -> ByteArray: return #[]

class PingReqPacket extends Packet:
  static TYPE ::= 12

  constructor:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    super TYPE

  variable_header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]

class PingRespPacket extends Packet:
  static TYPE ::= 13

  constructor:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    super TYPE

  variable_header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]

class DisconnectPacket extends Packet:
  static TYPE ::= 14

  constructor:
    super TYPE

  constructor.deserialize_ reader/reader.BufferedReader:
    super TYPE

  variable_header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]
