// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import net.tcp
import writer
import reader
import tls
import monitor

import .transport
import .packets


/**
A transport for backing an MQTT client with TCP or TLS/TCP.

Supports reconnecting to the same server if constructed with the connection information.
*/
abstract class TcpTransport implements Transport:
  socket_ /tcp.Socket? := null

  constructor socket/tcp.Socket:
    socket_ = socket

  constructor interface/tcp.Interface --host/string --port/int=1883:
    return ReconnectingTransport_ interface --host=host --port=port

  constructor.tls interface/tcp.Interface --host/string --port/int=8883
      --root_certificates/List=[]
      --server_name/string?=null
      --certificate/tls.Certificate?=null:
    return ReconnectingTlsTransport_ interface --host=host --port=port
      --root_certificates=root_certificates
      --server_name=server_name
      --certificate=certificate

  constructor.from_subclass_ .socket_:

  write bytes/ByteArray -> int:
    return socket_.write bytes

  read -> ByteArray?:
    return socket_.read

  close:
    if socket_: socket_.close
    socket_ = null

  is_closed -> bool:
    return socket_ == null

  supports_reconnect -> bool:
    return false

  reconnect -> none:
    throw "UNSUPPORTED"

class ReconnectingTransport_ extends TcpTransport:
  // Reconnection information.
  interface_ /tcp.Interface
  host_      /string
  port_      /int

  reconnecting_mutex /monitor.Mutex := monitor.Mutex

  constructor .interface_ --host/string --port/int=1883:
    host_ = host
    port_ = port
    super.from_subclass_ null
    reconnect

  write bytes/ByteArray -> int:
    return socket_.write bytes

  read -> ByteArray?:
    return socket_.read

  reconnect:
    old_socket := socket_
    reconnecting_mutex = monitor.Mutex
    reconnecting_mutex.do:
      if not identical old_socket socket_: return
      if old_socket: old_socket.close

      socket := new_connection_
      // Send messages immediately.
      socket.set_no_delay true

      // Set the new socket_ at the very end. This way we will try to
      // reconnect again if we are interrupted by a timeout.
      socket_ = socket

  new_connection_ -> tcp.Socket:
    return interface_.tcp_connect host_ port_

  supports_reconnect -> bool:
    return true

class ReconnectingTlsTransport_ extends ReconnectingTransport_:
  certificate_ /tls.Certificate?
  server_name_ /string?
  root_certificates_ /List

  constructor interface/tcp.Interface --host/string --port/int
      --root_certificates/List=[]
      --server_name/string?=null
      --certificate/tls.Certificate?=null:
    root_certificates_ = root_certificates
    server_name_ = server_name
    certificate_ = certificate
    super interface --host=host --port=port

  new_connection_ -> tcp.Socket:
    socket := interface_.tcp_connect host_ port_
    socket = tls.Socket.client socket
      --server_name=server_name_ or host_
      --certificate=certificate_
      --root_certificates=root_certificates_
    return socket

