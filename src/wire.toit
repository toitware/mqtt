// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import .errors
import .packets
import .topics

/**
A bounded MQTT 3.1.1 codec supporting QoS 0 and 1.

Decoding consumes the entire frame before returning, including publish payloads.
  The limit applies to the entire encoded packet and is checked before allocating
  its body. Transport errors propagate unchanged; malformed bodies are protocol errors.
*/
class Wire:
  max-packet-size/int

  constructor --.max-packet-size=16_384:
    if max-packet-size < 2: throw "INVALID_ARGUMENT"

  read reader/io.Reader -> Packet?:
    if not reader.try-ensure-buffered 1: return null
    header := io.Buffer
    first := reader.read-byte
    header.write-byte first
    size := 0
    terminated := false
    for index := 0; index < 4; index++:
      byte := reader.read-byte
      header.write-byte byte
      size |= (byte & 0x7f) << (7 * index)
      if byte & 0x80 == 0:
        if index > 0 and byte == 0: throw (ProtocolError "non-minimal length")
        terminated = true
        break
    if not terminated: throw (ProtocolError "remaining length exceeds four bytes")
    if size + header.size > max-packet-size: throw (CapacityError "packet bytes")
    kind := first >> 4
    flags := first & 15
    if kind != PublishPacket.TYPE and flags != (kind == 8 or kind == 10 ? 2 : 0):
      throw (ProtocolError "reserved flags")
    body := reader.read-bytes size
    // Use an isolated reader so even a malformed body cannot consume the next frame.
    header.write body
    frame := io.Reader header.bytes
    result/Packet? := null
    failure := catch:
      result = Packet.decode-frame_ frame
      if result is PublishPacket: (result as PublishPacket).payload
      if frame.try-ensure-buffered 1: throw "trailing packet bytes"
      validate_ result body
    if failure: throw (ProtocolError failure)
    return result

  encode packet/Packet -> ByteArray:
    bytes := packet.serialize
    if bytes.size > max-packet-size: throw (CapacityError "packet bytes")
    // The same rules apply to locally constructed and received packets.
    read (io.Reader bytes)
    return bytes

  validate_ packet/Packet body/ByteArray:
    if packet is ConnectPacket:
      p := packet as ConnectPacket
      flags := body[7]
      if flags & 1 != 0: throw "reserved connect flag"
      if flags & 4 == 0 and flags & 0x38 != 0: throw "will flags without will"
      if p.password and not p.username: throw "password without username"
      validate-string p.client-id --allow-empty=p.clean-session
      if p.username: validate-string p.username --allow-empty
      if p.last-will:
        validate-topic p.last-will.topic
        if not 0 <= p.last-will.qos <= 1: throw "unsupported will qos"
    else if packet is ConnAckPacket:
      p := packet as ConnAckPacket
      if body[0] & 0xfe != 0: throw "reserved connack flags"
      if not 0 <= p.return-code <= 5: throw "connack return code"
      if p.return-code != 0 and p.session-present: throw "session present on rejection"
    else if packet is PublishPacket:
      p := packet as PublishPacket
      validate-topic p.topic
      if not 0 <= p.qos <= 1: throw "unsupported publish qos"
      if p.qos == 0 and p.duplicate: throw "duplicate qos zero"
      if p.qos == 1: validate-id_ p.packet-id
    else if packet is SubscribePacket:
      p := packet as SubscribePacket
      validate-id_ p.packet-id
      if p.topics.is-empty: throw "empty subscription"
      p.topics.do:
        validate-filter it.topic
        if not 0 <= it.max-qos <= 1: throw "unsupported subscription qos"
    else if packet is UnsubscribePacket:
      p := packet as UnsubscribePacket
      validate-id_ p.packet-id
      if p.topics.is-empty: throw "empty unsubscription"
      p.topics.do: validate-filter it
    else if packet is AckPacket:
      validate-id_ (packet as AckPacket).packet-id
      if packet is SubAckPacket:
        p := packet as SubAckPacket
        if p.qos.is-empty: throw "empty suback"
        p.qos.do:
          if it != 0 and it != 1 and it != 0x80: throw "unsupported suback qos"

  validate-id_ id/int:
    if not 1 <= id <= 0xffff: throw "invalid packet id"
