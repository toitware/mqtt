// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import io
import mqtt.link show *
import mqtt.packets show *
import mqtt.wire show *

/** A scripted byte stream. Tests explicitly choose every broker response. */
monitor Pipe:
  chunks/Deque := Deque
  closed := false
  write bytes/ByteArray:
    if closed: throw "BROKEN_PIPE"
    chunks.add bytes
  read -> ByteArray?:
    await: closed or not chunks.is-empty
    if chunks.is-empty: return null
    return chunks.remove-first
  close:
    closed = true

class MemoryLink implements Link:
  incoming/Pipe
  outgoing/Pipe
  chunk-size/int
  constructor .incoming .outgoing --.chunk-size=65_536:
  read -> ByteArray?: return incoming.read
  write bytes/ByteArray -> int:
    size := min bytes.size chunk-size
    outgoing.write bytes[..size]
    return size
  close -> none:
    incoming.close
    outgoing.close

class ScriptConnector implements Connector:
  links/Deque := Deque
  attempts := 0
  open -> Link:
    attempts++
    if links.is-empty: throw "OFFLINE"
    return links.remove-first

class Peer:
  client-link/MemoryLink
  link/MemoryLink
  reader_/io.Reader
  wire_/Wire := Wire
  constructor --fragmented/bool=false:
    to-client := Pipe
    to-peer := Pipe
    client-link = MemoryLink to-client to-peer --chunk-size=(fragmented ? 1 : 65_536)
    link = MemoryLink to-peer to-client
    reader_ = PeerReader_ link
  receive -> Packet?: return wire_.read reader_
  send packet/Packet:
    link.write (wire_.encode packet)
  send-bytes bytes/ByteArray:
    link.write bytes
  accept --session-present/bool=false:
    request := receive
    if request is not ConnectPacket: throw "EXPECTED_CONNECT"
    send (ConnAckPacket --session-present=session-present)
  close:
    link.close

class PeerReader_ extends io.Reader:
  link/Link
  constructor .link:
  read_ -> ByteArray?: return link.read
