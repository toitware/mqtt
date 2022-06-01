// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple transport for testing.
*/

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

  is_closed := false

  client_write bytes/ByteArray -> none:
    client_to_broker_data_.add bytes

  client_read -> ByteArray?:
    await: broker_to_client_data_.size > 0 or is_closed
    if broker_to_client_data_.is_empty: return null
    result := broker_to_client_data_.remove_first
    return result

  client_close:
    is_closed = true

  client_is_closed -> bool:
    return is_closed


  broker_write bytes/ByteArray -> none:
    broker_to_client_data_.add bytes

  broker_read -> ByteArray?:
    await: client_to_broker_data_.size > 0 or is_closed
    if client_to_broker_data_.is_empty: return null
    return client_to_broker_data_.remove_first

  broker_close:
    is_closed = true

  broker_is_closed -> bool:
    return is_closed

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


class LoggingTransport implements mqtt.Transport:
  intercepted_bytes_ := []
  wrapped_ /mqtt.Transport

  constructor .wrapped_:

  write bytes/ByteArray -> int:
    // We assume that all bytes are always fully written.
    written := wrapped_.write bytes
    intercepted_bytes_.add [ "write", bytes[0..written] ]
    return written

  read -> ByteArray?:
    result := wrapped_.read
    intercepted_bytes_.add [ "read", result ]
    return result

  close -> none:
    intercepted_bytes_.add [ "close" ]
    wrapped_.close

  supports_reconnect -> bool:
    return true

  reconnect -> none:
    intercepted_bytes_.add [ "reconnect" ]
    wrapped_.reconnect

  is_closed -> bool:
    return wrapped_.is_closed

  clear -> none:
    intercepted_bytes_.clear

  packets -> List:
    result := []
    read_pipe := Pipe_
    read_reader := reader.BufferedReader read_pipe
    write_pipe := Pipe_
    write_reader := reader.BufferedReader write_pipe

    done := monitor.Semaphore
    task --background::
      while packet := mqtt.Packet.deserialize read_reader:
        result.add [ "read", packet ]
      done.up
    task --background::
      while packet := mqtt.Packet.deserialize write_reader:
        result.add [ "write", packet ]
      done.up
    intercepted_bytes_.do:
      if it[0] == "read": read_pipe.write it[1]
      else: write_pipe.write it[1]
    read_pipe.close
    write_pipe.close
    done.down
    done.down
    return result
