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
*/
class TcpTransport implements Transport:
  socket_/tcp.Socket
  writer_/writer.Writer
  reader_/reader.BufferedReader

  constructor .socket_:
    writer_ = writer.Writer socket_
    reader_ = reader.BufferedReader socket_
    // Send messages immediately.
    socket_.set_no_delay true

  send packet/Packet:
    writer_.write packet.serialize

  receive --timeout/Duration?=null -> Packet?:
    catch --unwind=(: it != DEADLINE_EXCEEDED_ERROR):
      with_timeout timeout:
        return Packet.deserialize reader_
    return null

/**
A transport for backing an MQTT client with TCP or TLS/TCP.

Supports reconnecting to the same server.
*/
class ReconnectTcpTransport implements ReconnectTransport:
  interface_   /tcp.Interface
  use_tls_     /bool
  certificate_ /tls.Certificate?
  server_name_ /string?
  root_certificates_ /List

  host_ /string
  port_ /int

  transport_ /TcpTransport? := null
  reconnecting_mutex /monitor.Mutex? := null

  constructor .interface_ --host/string --port/int=1883:
    host_ = host
    port_ = port
    use_tls_ = false
    root_certificates_ = []
    server_name_ = null
    certificate_ = null

    transport_ = new_transport_

  constructor.tls .interface_ --host/string --port/int=8883
      --root_certificates/List=[]
      --server_name/string?=null
      --certificate/tls.Certificate?=null:
    host_ = host
    port_ = port
    use_tls_ = true
    root_certificates_ = root_certificates
    server_name_ = server_name
    certificate_ = certificate

    transport_ = new_transport_

  send packet/Packet:
    transport_.send packet

  receive --timeout/Duration?=null -> Packet?:
    return transport_.receive --timeout=timeout

  reconnect:
    if reconnecting_mutex:
      reconnecting_mutex.do: return

    reconnecting_mutex = monitor.Mutex
    reconnecting_mutex.do:
      try:
        if transport_:
          close_transport_

        // TODO(florian): implement exponential back-off.
        transport_ = new_transport_
      finally:
        reconnecting_mutex = null

  new_connection_ -> tcp.Socket:
    socket := interface_.tcp_connect host_ port_
    if use_tls_:
      socket = tls.Socket.client socket
        --server_name=server_name_ or host_
        --certificate=certificate_
        --root_certificates=root_certificates_
    return socket

  new_transport_ -> TcpTransport:
    socket := new_connection_
    return TcpTransport socket

  close_transport_ -> none:
    assert: transport_
    transport := transport_
    transport_ = null
    transport.socket_.close
