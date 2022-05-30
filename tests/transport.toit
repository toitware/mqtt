// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple transport for testing.
*/

import mqtt.transport as mqtt
import .broker as broker
import monitor

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

class TestBrokerTransport implements broker.Transport:
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
    pipe := channel_.receive
    callback.call (TestBrokerTransport pipe)

  connect -> TestTransportPipe:
    if is_closed:
      throw "Transport is closed"

    pipe := TestTransportPipe
    channel_.send pipe
    return pipe

  close -> none:
    is_closed = true


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
