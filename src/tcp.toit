// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import monitor
import net
import net.tcp
import tls

import .broker
import .transport
import .packets


/**
A transport for backing an MQTT client with TCP or TLS/TCP.

Supports reconnecting to the same server if constructed with the connection information.
*/
class TcpTransport implements Transport BrokerTransport:
  socket_ /tcp.Socket? := null
  socket-reader_ /io.Reader? := null
  socket-writer_ /io.Writer? := null

  constructor socket/tcp.Socket:
    set-socket_ socket

  /** Deprecated. Use $(constructor --net-open --host) instead. */
  constructor network/net.Interface --host/string --port/int=1883:
    return ReconnectingTransport_ network --net-open=null --host=host --port=port

  constructor --host/string --port/int=1883 --net-open/Lambda=(:: net.open):
    return ReconnectingTransport_ null --net-open=net-open --host=host --port=port

  /** Deprecated. Use $(TcpTransport.tls --net-open --host) instead. */
  constructor.tls network/net.Interface --host/string --port/int=8883
      --root-certificates/List=[]
      --server-name/string?=null
      --certificate/tls.Certificate?=null:
    return ReconnectingTlsTransport_ network --net-open=null --host=host --port=port
      --root-certificates=root-certificates
      --server-name=server-name
      --certificate=certificate

  constructor.tls --host/string --port/int=8883 --net-open/Lambda=(:: net.open)
      --root-certificates/List=[]
      --server-name/string?=null
      --certificate/tls.Certificate?=null:
    return ReconnectingTlsTransport_ null --net-open=net-open --host=host --port=port
      --root-certificates=root-certificates
      --server-name=server-name
      --certificate=certificate

  constructor.from-subclass_:

  set-socket_ socket/tcp.Socket?:
    socket_ = socket
    if socket_:
      socket-reader_ = socket_.in
      socket-writer_ = socket_.out

  write bytes/ByteArray -> int:
    return socket-writer_.try-write bytes 0 bytes.size

  read -> ByteArray?:
    return socket-reader_.read

  close:
    if socket_: socket_.close
    socket_ = null
    socket-reader_ = null
    socket-writer_ = null

  is-closed -> bool:
    return socket_ == null

  supports-reconnect -> bool:
    return false

  reconnect -> none:
    throw "UNSUPPORTED"

  disconnect -> none:
    throw "UNSUPPORTED"

class ReconnectingTransport_ extends TcpTransport:
  // Reconnection information.
  network_ /net.Interface? := null
  host_    /string
  port_    /int
  open_    /Lambda?

  reconnecting-mutex_ /monitor.Mutex := monitor.Mutex

  constructor .network_ --net-open/Lambda? --host/string --port/int=1883:
    if not network_ and not net-open: throw "Either network or net_open must be provided"
    if network_ and net-open: throw "Only one of network or net_open must be provided"
    host_ = host
    port_ = port
    open_ = net-open
    super.from-subclass_
    try:
      reconnect
    finally: | is-exception _ |
      if is-exception: close

  close -> none:
    super
    // Only close the network if we were the ones who opened it.
    if open_ and network_:
      network_.close
      network_ = null

  reconnect:
    if not network_: network_ = open_.call
    old-socket := socket_
    reconnecting-mutex_.do:
      if not identical old-socket socket_: return
      if old-socket: old-socket.close

      // TODO(florian): we dynamically try to call `no_delay = true` or
      // `set_no_delay true`. The first is for newer Toit versions, the latter
      // for older versions.
      // Eventually we should stop supporting old versions.
      socket /any := new-connection_
      // Send messages immediately.
      exception := catch:
        socket.no-delay = true
      if exception:
        socket.set-no-delay true

      // Set the new socket_ at the very end. This way we will try to
      // reconnect again if we are interrupted by a timeout.
      set-socket_ socket

  new-connection_ -> tcp.Socket:
    return network_.tcp-connect host_ port_

  supports-reconnect -> bool:
    return true

  disconnect:
    if not open_: return
    if network_:
      network := network_
      network_ = null
      network.close

class ReconnectingTlsTransport_ extends ReconnectingTransport_:
  certificate_ /tls.Certificate?
  server-name_ /string?
  root-certificates_ /List

  constructor network/net.Interface? --net-open/Lambda? --host/string --port/int
      --root-certificates/List=[]
      --server-name/string?=null
      --certificate/tls.Certificate?=null:
    root-certificates_ = root-certificates
    server-name_ = server-name
    certificate_ = certificate
    super network --net-open=net-open --host=host --port=port

  new-connection_ -> tcp.Socket:
    socket := network_.tcp-connect host_ port_
    socket = tls.Socket.client socket
      --server-name=server-name_ or host_
      --certificate=certificate_
      --root-certificates=root-certificates_
    return socket
