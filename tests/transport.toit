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
    pipe_.client_write bytes
    return bytes.size

  read -> ByteArray?:
    return pipe_.client_read

  close -> none:
    pipe_.client_close

  supports_reconnect -> bool:
    return true

  reconnect -> none:
    pipe_ = server_.connect

  disconnect -> none:

  is_closed -> bool:
    return pipe_.client_is_closed

class TestBrokerTransport implements broker.BrokerTransport:
  pipe_ /TestTransportPipe

  constructor .pipe_:

  write bytes/ByteArray -> int:
    pipe_.broker_write bytes
    return bytes.size

  read -> ByteArray?:
    return pipe_.broker_read

  close -> none:
    pipe_.broker_close

class TestServerTransport implements broker.ServerTransport:
  channel_ /monitor.Channel := monitor.Channel 5

  is_closed /bool := false

  listen callback/Lambda -> none:
    while pipe := channel_.receive:
      callback.call (TestBrokerTransport pipe)

  connect -> TestTransportPipe:
    if is_closed:
      throw "Transport is closed"

    pipe := TestTransportPipe
    channel_.send pipe
    return pipe

  close -> none:
    is_closed = true
    channel_.send null

monitor TestTransportPipe:
  client_to_broker_data_ /Deque := Deque
  broker_to_client_data_ /Deque := Deque

  closed_from_client_ /bool := false
  closed_from_broker_ /bool := false

  client_write bytes/ByteArray -> none:
    if is_closed_: throw "CLOSED"
    client_to_broker_data_.add bytes

  client_read -> ByteArray?:
    await: broker_to_client_data_.size > 0 or is_closed_
    if closed_from_client_: throw "CLOSED"
    if broker_to_client_data_.is_empty: return null
    result := broker_to_client_data_.remove_first
    return result

  client_close:
    closed_from_client_ = true

  client_is_closed -> bool:
    return is_closed_

  broker_write bytes/ByteArray -> none:
    if is_closed_: throw "CLOSED"
    broker_to_client_data_.add bytes

  broker_read -> ByteArray?:
    await: client_to_broker_data_.size > 0 or is_closed_
    if closed_from_broker_: throw "CLOSED"
    if client_to_broker_data_.is_empty: return null
    return client_to_broker_data_.remove_first

  broker_close:
    closed_from_broker_ = true

  broker_is_closed -> bool:
    return is_closed_

  is_closed_ -> bool:
    return closed_from_client_ or closed_from_broker_

monitor Pipe_ implements reader.Reader:
  data_ /any := null
  is_closed_ /bool := false

  read -> ByteArray?:
    await: data_ or is_closed_
    if is_closed_: return null
    result := data_
    data_ = null
    return result

  close:
    is_closed_ = true

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
  pending_bytes_read_ := []
  // The bytes that have been written but aren't yet yielding a full packet.
  pending_bytes_write_ := []

  wrapped_ /mqtt.Transport
  read_filter_ /Lambda?
  write_filter_ /Lambda?

  read_task_ := null
  read_channel_ /monitor.Channel := monitor.Channel 20

  /**
  Keeps track of writes that need several `write` calls.
  We assume that the first attempt to $write always contains a full packet.
  Further attempts just write the rest of the packet.
  */
  remaining_to_write_ /int := 0
  packet_being_written_ /mqtt.Packet? := null

  constructor .wrapped_ --read_filter/Lambda?=null --write_filter/Lambda?=null:
    read_filter_ = read_filter
    write_filter_ = write_filter
    start_reading_

  start_reading_ -> none:
    if read_task_:
      read_task_.cancel
      old_channel := read_channel_
      read_channel_ = monitor.Channel 20
      old_channel.send null  // In case something is listening.

    read_task_ = task --background::
      should_unwind := false
      exception_or_null := catch --unwind=should_unwind:
        // Some pending bytes that aren't in the intercepted reader anymore, but still
        // need to be sent.
        pending /ByteArray? := null
        intercepting_reader := InterceptingReader_ wrapped_
        buffered := reader.BufferedReader intercepting_reader
        while packet := mqtt.Packet.deserialize buffered:
          if read_filter_:
            packet = read_filter_.call packet
          if not packet: continue
          activity_.add [ "read", packet, Time.monotonic_us ]
          serialized := packet.serialize
          intercepted_bytes := intercepting_reader.intercepted
          forwarded_count := 0
          forwarded_bytes := #[]
          while forwarded_count < serialized.size:
            if not pending: pending = intercepted_bytes.remove_first
            to_forward := ?
            if pending.size + forwarded_count < serialized.size:
              to_forward = pending
              pending = null
            else:
              to_forward = pending[0..serialized.size - forwarded_count]
              pending = pending[serialized.size - forwarded_count..]
            read_channel_.send to_forward
            forwarded_count += to_forward.size
            forwarded_bytes += to_forward
          if forwarded_bytes != serialized:
            should_unwind = true
            throw "Serialized packet doesn't match forwarded bytes"

      if exception_or_null is ByteArray: throw "UNEXPECTED EXCEPTION TYPE"
      read_channel_.send exception_or_null

  write byte_array/ByteArray -> int:
    reader := reader.BufferedReader (bytes.Reader byte_array)
    if remaining_to_write_ == 0:
      // We assume that the first attempt to write bytes represent a full packet.
      packet := mqtt.Packet.deserialize reader
      if write_filter_:
        packet = write_filter_.call packet
      if not packet: return byte_array.size
      remaining_to_write_ = byte_array.size
      packet_being_written_ = packet

    written := wrapped_.write byte_array
    remaining_to_write_ -= written
    if remaining_to_write_ == 0:
      activity_.add [ "write", packet_being_written_, Time.monotonic_us ]
      packet_being_written_ = null
    return written

  read -> ByteArray?:
    channel := read_channel_
    result := channel.receive
    if channel != read_channel_:
      // The underlying channel was replaced.
      // Just start reading from the new one.
      return read
    if not result: return result
    if result is not ByteArray: throw result
    return result

  close -> none:
    activity_.add [ "close", Time.monotonic_us ]
    wrapped_.close

  supports_reconnect -> bool:
    return wrapped_.supports_reconnect
    return true

  reconnect -> none:
    activity_.add [ "reconnect", Time.monotonic_us ]
    wrapped_.reconnect
    remaining_to_write_ = 0
    start_reading_

  disconnect -> none:
    activity_.add [ "disconnect", Time.monotonic_us ]
    wrapped_.disconnect

  is_closed -> bool:
    return wrapped_.is_closed

  clear -> none:
    activity_.clear

  activity -> List:
    return activity_

class CallbackTestTransport implements mqtt.Transport:
  wrapped_ /mqtt.Transport

  on_reconnect /Lambda? := null
  on_disconnect /Lambda? := null
  on_write /Lambda? := null
  on_read /Lambda? := null

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    if on_write: on_write.call bytes
    return wrapped_.write bytes

  read -> ByteArray?:
    if on_read: return on_read.call wrapped_
    return wrapped_.read

  close -> none: wrapped_.close

  supports_reconnect -> bool: return wrapped_.supports_reconnect

  reconnect -> none:
    if on_reconnect: on_reconnect.call
    wrapped_.reconnect

  disconnect -> none:
    if on_disconnect: on_disconnect.call
    wrapped_.disconnect

  is_closed -> bool: return wrapped_.is_closed
