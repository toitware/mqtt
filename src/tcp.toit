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
  constructor socket/tcp.Socket:
    return SocketTcpTransport_ socket

  constructor interface/tcp.Interface --host/string --port/int=1883:
    return ReconnectingTcpTransport_ interface --host=host --port=port

  constructor.tls interface/tcp.Interface --host/string --port/int=8883
      --root_certificates/List=[]
      --server_name/string?=null
      --certificate/tls.Certificate?=null:
    return ReconnectingTlsTcpTransport_ interface --host=host --port=port
      --root_certificates=root_certificates
      --server_name=server_name
      --certificate=certificate

  constructor.from_subclass_:

  abstract send packet/Packet
  abstract receive --timeout/Duration?=null -> Packet?

class SocketTcpTransport_ extends TcpTransport:
  socket_ /tcp.Socket
  writer_ /writer.Writer
  reader_ /reader.BufferedReader

  transport_ /TcpTransport? := null
  reconnecting_mutex /monitor.Mutex? := null

  constructor .socket_:
    writer_ = writer.Writer socket_
    reader_ = reader.BufferedReader socket_
    // Send messages immediately.
    socket_.set_no_delay true
    super.from_subclass_

  send packet/Packet:
    writer_.write packet.serialize

  receive --timeout/Duration?=null -> Packet?:
    catch --unwind=(: it != DEADLINE_EXCEEDED_ERROR):
      with_timeout timeout:
        return Packet.deserialize reader_
    return null

  close:
    // TODO(florian): should we close a socket we haven't created?
    socket_.close

class ReconnectingTcpTransport_ extends TcpTransport implements ReconnectingTransport:
  // Reconnection information.
  interface_ /tcp.Interface
  host_      /string
  port_      /int

  // The current connection.
  socket_ /tcp.Socket? := null
  writer_ /writer.Writer? := null
  reader_ /reader.BufferedReader? := null

  reconnecting_mutex /monitor.Mutex := monitor.Mutex

  constructor .interface_ --host/string --port/int=1883:
    host_ = host
    port_ = port
    super.from_subclass_
    reconnect

  send packet/Packet:
    writer_.write packet.serialize

  receive --timeout/Duration?=null -> Packet?:
    catch --unwind=(: it != DEADLINE_EXCEEDED_ERROR):
      with_timeout timeout:
        return Packet.deserialize reader_
    return null

  reconnect:
    // TODO(florian): implement retries and exponential back-off.
    old_socket := socket_
    reconnecting_mutex = monitor.Mutex
    reconnecting_mutex.do:
      if not identical old_socket socket_: return
      if old_socket: old_socket.close

      socket := new_connection_
      writer_ = writer.Writer socket
      reader_ = reader.BufferedReader socket
      // Send messages immediately.
      socket.set_no_delay true

      // Set the new socket_ at the very end. This way we will try to
      // reconnect again if we are interrupted by a timeout.
      socket_ = socket

  new_connection_ -> tcp.Socket:
    return interface_.tcp_connect host_ port_

  close:
    if socket_: socket_.close
    socket_ = null
    writer_ = null
    reader_ = null


class ReconnectingTlsTcpTransport_ extends ReconnectingTcpTransport_:
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

