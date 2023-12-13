// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple transport for testing.
*/

import bytes
import mqtt.transport as mqtt
import mqtt.broker as broker
import mqtt.packets as mqtt
import monitor
import reader

class TestClientTransport implements mqtt.Transport:
  server_ /TestServerTransport
  pipe_ /TestTransportPipe? := null

  constructor .server_:
    reconnect

  write bytes/ByteArray -> int:
    pipe_.client-write bytes
    return bytes.size

  read -> ByteArray?:
    return pipe_.client-read

  close -> none:
    pipe_.client-close

  supports-reconnect -> bool:
    return true

  reconnect -> none:
    pipe_ = server_.connect

  disconnect -> none:

  is-closed -> bool:
    return pipe_.client-is-closed

class TestBrokerTransport implements broker.BrokerTransport:
  pipe_ /TestTransportPipe

  constructor .pipe_:

  write bytes/ByteArray -> int:
    pipe_.broker-write bytes
    return bytes.size

  read -> ByteArray?:
    return pipe_.broker-read

  close -> none:
    pipe_.broker-close

class TestServerTransport implements broker.ServerTransport:
  channel_ /monitor.Channel := monitor.Channel 5

  is-closed /bool := false

  listen callback/Lambda -> none:
    while pipe := channel_.receive:
      callback.call (TestBrokerTransport pipe)

  connect -> TestTransportPipe:
    if is-closed:
      throw "Transport is closed"

    pipe := TestTransportPipe
    channel_.send pipe
    return pipe

  close -> none:
    is-closed = true
    channel_.send null

monitor TestTransportPipe:
  client-to-broker-data_ /Deque := Deque
  broker-to-client-data_ /Deque := Deque

  closed-from-client_ /bool := false
  closed-from-broker_ /bool := false

  client-write bytes/ByteArray -> none:
    if is-closed_: throw "CLOSED"
    client-to-broker-data_.add bytes

  client-read -> ByteArray?:
    await: broker-to-client-data_.size > 0 or is-closed_
    if closed-from-client_: throw "CLOSED"
    if broker-to-client-data_.is-empty: return null
    result := broker-to-client-data_.remove-first
    return result

  client-close:
    closed-from-client_ = true

  client-is-closed -> bool:
    return is-closed_

  broker-write bytes/ByteArray -> none:
    if is-closed_: throw "CLOSED"
    broker-to-client-data_.add bytes

  broker-read -> ByteArray?:
    await: client-to-broker-data_.size > 0 or is-closed_
    if closed-from-broker_: throw "CLOSED"
    if client-to-broker-data_.is-empty: return null
    return client-to-broker-data_.remove-first

  broker-close:
    closed-from-broker_ = true

  broker-is-closed -> bool:
    return is-closed_

  is-closed_ -> bool:
    return closed-from-client_ or closed-from-broker_

monitor Pipe_ implements reader.Reader:
  data_ /any := null
  is-closed_ /bool := false

  read -> ByteArray?:
    await: data_ or is-closed_
    if is-closed_: return null
    result := data_
    data_ = null
    return result

  close:
    is-closed_ = true

  write bytes/ByteArray -> none:
    await: not data_
    data_ = bytes

class InterceptingReader_ implements reader.Reader:
  wrapped_ /reader.Reader
  intercepted /Deque := Deque

  constructor .wrapped_:

  read -> ByteArray?:
    bytes := wrapped_.read
    intercepted.add bytes
    return bytes

class TestTransport implements mqtt.Transport:
  activity_ := []

  // The bytes that have been read but aren't yet yielding a full packet.
  pending-bytes-read_ := []
  // The bytes that have been written but aren't yet yielding a full packet.
  pending-bytes-write_ := []

  wrapped_ /mqtt.Transport
  read-filter_ /Lambda?
  write-filter_ /Lambda?

  read-task_ := null
  read-channel_ /monitor.Channel := monitor.Channel 20

  /**
  Keeps track of writes that need several `write` calls.
  We assume that the first attempt to $write always contains a full packet.
  Further attempts just write the rest of the packet.
  */
  remaining-to-write_ /int := 0
  packet-being-written_ /mqtt.Packet? := null

  constructor .wrapped_ --read-filter/Lambda?=null --write-filter/Lambda?=null:
    read-filter_ = read-filter
    write-filter_ = write-filter
    start-reading_

  start-reading_ -> none:
    if read-task_:
      read-task_.cancel
      old-channel := read-channel_
      read-channel_ = monitor.Channel 20
      old-channel.send null  // In case something is listening.

    read-task_ = task --background::
      should-unwind := false
      exception-or-null := catch --unwind=should-unwind:
        // Some pending bytes that aren't in the intercepted reader anymore, but still
        // need to be sent.
        pending /ByteArray? := null
        intercepting-reader := InterceptingReader_ wrapped_
        buffered := reader.BufferedReader intercepting-reader
        while packet := mqtt.Packet.deserialize buffered:
          if read-filter_:
            packet = read-filter_.call packet
          if not packet: continue
          activity_.add [ "read", packet, Time.monotonic-us ]
          serialized := packet.serialize
          intercepted-bytes := intercepting-reader.intercepted
          forwarded-count := 0
          forwarded-bytes := #[]
          while forwarded-count < serialized.size:
            if not pending: pending = intercepted-bytes.remove-first
            to-forward := ?
            if pending.size + forwarded-count < serialized.size:
              to-forward = pending
              pending = null
            else:
              to-forward = pending[0..serialized.size - forwarded-count]
              pending = pending[serialized.size - forwarded-count..]
            read-channel_.send to-forward
            forwarded-count += to-forward.size
            forwarded-bytes += to-forward
          if forwarded-bytes != serialized:
            should-unwind = true
            throw "Serialized packet doesn't match forwarded bytes"

      if exception-or-null is ByteArray: throw "UNEXPECTED EXCEPTION TYPE"
      read-channel_.send exception-or-null

  write byte-array/ByteArray -> int:
    reader := reader.BufferedReader (bytes.Reader byte-array)
    if remaining-to-write_ == 0:
      // We assume that the first attempt to write bytes represent a full packet.
      packet := mqtt.Packet.deserialize reader
      if write-filter_:
        packet = write-filter_.call packet
      if not packet: return byte-array.size
      remaining-to-write_ = byte-array.size
      packet-being-written_ = packet

    written := wrapped_.write byte-array
    remaining-to-write_ -= written
    if remaining-to-write_ == 0:
      activity_.add [ "write", packet-being-written_, Time.monotonic-us ]
      packet-being-written_ = null
    return written

  read -> ByteArray?:
    channel := read-channel_
    result := channel.receive
    if channel != read-channel_:
      // The underlying channel was replaced.
      // Just start reading from the new one.
      return read
    if not result: return result
    if result is not ByteArray: throw result
    return result

  close -> none:
    activity_.add [ "close", Time.monotonic-us ]
    wrapped_.close

  supports-reconnect -> bool:
    return wrapped_.supports-reconnect
    return true

  reconnect -> none:
    activity_.add [ "reconnect", Time.monotonic-us ]
    wrapped_.reconnect
    remaining-to-write_ = 0
    start-reading_

  disconnect -> none:
    activity_.add [ "disconnect", Time.monotonic-us ]
    wrapped_.disconnect

  is-closed -> bool:
    return wrapped_.is-closed

  clear -> none:
    activity_.clear

  activity -> List:
    return activity_

class CallbackTestTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport

  on-reconnect /Lambda? := null
  on-disconnect /Lambda? := null
  on-write /Lambda? := null
  on-after-write /Lambda? := null
  on-read /Lambda? := null
  on-after-read /Lambda? := null

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    if on-write: on-write.call bytes
    result := wrapped_.write bytes
    if on-after-write: on-after-write.call bytes result
    return result

  read -> ByteArray?:
    if on-read: return on-read.call wrapped_
    result := wrapped_.read
    if on-after-read: on-after-read.call result
    return result

  close -> none: wrapped_.close

  supports-reconnect -> bool: return wrapped_.supports-reconnect

  reconnect -> none:
    if on-reconnect: on-reconnect.call
    wrapped_.reconnect

  disconnect -> none:
    if on-disconnect: on-disconnect.call
    wrapped_.disconnect

  is-closed -> bool: return wrapped_.is-closed
