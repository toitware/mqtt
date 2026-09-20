// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import io
import mqtt.wire show *
import mqtt.packets show *
import mqtt.errors show *
import mqtt.topics show *

/** A reader that makes every byte a separate transport read. */
class Fragments extends io.Reader:
  bytes/ByteArray
  offset := 0
  constructor .bytes:
  read_ -> ByteArray?:
    if offset == bytes.size: return null
    return bytes[offset++..offset]

main:
  wire := Wire
  packet := PublishPacket "room/temp" #[1, 2, 3] --qos=1 --retain=false --packet-id=42
  encoded := wire.encode packet
  received := wire.read (Fragments encoded)
  expect-equals #[1, 2, 3] received.payload
  expect-null (wire.read (io.Reader #[]))

  // Invalid framing, flags, IDs, and unsupported QoS must fail at the boundary.
  [
    #[0xd0, 2, 0xc0, 0],
    #[0xd1, 0],
    #[0x40, 2, 0, 0],
    #[0xd0, 0x80, 0x80, 0x80, 0x80],
    #[0xd0, 0x80, 0],
    #[0x20, 2, 1, 4],
    #[0x36, 5, 0, 1, 'x', 0, 1],
  ].do: | bytes |
    failure := catch: wire.read (io.Reader bytes)
    expect failure is ProtocolError

  // The size limit is enforced without waiting for an oversized body.
  failure := catch: (Wire --max-packet-size=8).read (io.Reader #[0x30, 127])
  expect failure is CapacityError

  // Truncation is never mistaken for a complete, acknowledged message.
  encoded.size.repeat: | length |
    if length == 0: continue.repeat
    expect-not-null (catch: wire.read (Fragments encoded[..length]))

  expect (matches "a/#" "a")
  expect (matches "a/+/c" "a/b/c")
  expect (matches "a/+" "a/")
  expect-not (matches "#" "\$SYS/status")
  expect (matches "\$SYS/#" "\$SYS/status")
  ["", "a+", "a/#/b", "a/b#"].do:
    expect-not-null (catch: validate-filter it)
