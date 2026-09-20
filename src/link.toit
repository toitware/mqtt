// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import monitor
import net
import net.tcp
import tls
import .errors
import .packets
import .wire

/**
A single connection's byte stream. A closed link is never reopened.

Close is idempotent, does not throw, and unblocks pending reads and writes.
  Writes may be partial but must make progress or throw.
*/
interface Link:
  read -> ByteArray?
  write bytes/ByteArray -> int
  close -> none

/** Creates a fresh, owned link for each attempt; cleans up failed attempts. */
interface Connector:
  open -> Link

/** Accepts independent links. Close unblocks accept; null means the listener ended. */
interface Listener:
  accept -> Link?
  close -> none

/** Opens a fresh TCP connection, optionally with TLS. */
class TcpConnector implements Connector:
  host_/string
  port_/int
  net-open_/Lambda
  use-tls_/bool
  roots_/List
  server-name_/string?
  certificate_/tls.Certificate?

  constructor --host/string --port/int=1883 --net-open/Lambda=(:: net.open)
      --secure/bool=false --root-certificates/List=[] --server-name/string?=null
      --certificate/tls.Certificate?=null:
    host_ = host
    port_ = port
    net-open_ = net-open
    use-tls_ = secure
    roots_ = root-certificates
    server-name_ = server-name
    certificate_ = certificate

  open -> Link:
    network/net.Interface? := null
    socket/tcp.Socket? := null
    success := false
    try:
      network = net-open_.call
      socket = network.tcp-connect host_ port_
      if use-tls_:
        socket = tls.Socket.client socket --server-name=(server-name_ or host_)
            --root-certificates=roots_
            --certificate=certificate_
      socket.no-delay = true
      result := SocketLink socket --network=network
      success = true
      return result
    finally:
      if not success:
        critical-do:
          if socket: catch: socket.close
          if network: catch: network.close

/** Adapts a socket, optionally taking ownership of its network interface. */
class SocketLink implements Link:
  socket_/tcp.Socket
  network_/net.Interface?
  closed_ := false
  constructor .socket_ --network/net.Interface?=null:
    network_ = network
  read -> ByteArray?: return socket_.in.read
  write bytes/ByteArray -> int:
    return socket_.out.try-write bytes 0 bytes.size
  close -> none:
    if closed_: return
    closed_ = true
    catch: socket_.close
    if network_: catch: network_.close

/** Adapts a listening socket; its network interface remains owned by the caller. */
class TcpListener implements Listener:
  socket_/tcp.ServerSocket
  closed_/bool := false
  constructor .socket_:
  accept -> Link?:
    while not closed_:
      socket/tcp.Socket? := null
      failure := catch: socket = socket_.accept
      if closed_: return null
      if failure: throw failure
      // The native listener can return null after a readiness race.
      if socket: return SocketLink socket
    return null
  close -> none:
    closed_ = true
    catch: socket_.close

class LinkReader_ extends io.Reader:
  link_/Link
  constructor .link_:
  read_ -> ByteArray?: return link_.read

class LinkWriter_ extends io.Writer:
  link_/Link
  constructor .link_:
  try-write_ data/io.Data from/int to/int -> int:
    bytes := ByteArray (to - from)
    data.write-to-byte-array bytes --at=0 from to
    count := link_.write bytes
    if count <= 0 or count > bytes.size: throw "INVALID_WRITE_PROGRESS"
    return count

/** One connection's codec and writer lock, with no reconnection policy. */
class PacketConnection_:
  link/Link
  wire_/Wire
  reader_/io.Reader
  writer_/io.Writer
  writing_/monitor.Mutex := monitor.Mutex
  write-timeout_/Duration

  constructor .link wire/Wire --write-timeout/Duration:
    wire_ = wire
    write-timeout_ = write-timeout
    reader_ = LinkReader_ link
    writer_ = LinkWriter_ link

  read -> Packet?:
    result/Packet? := null
    failure := catch: result = wire_.read reader_
    if failure:
      if failure is MqttError: throw failure
      throw (ConnectionError "READ_FAILED" failure)
    return result

  write packet/Packet -> none:
    bytes := wire_.encode packet
    failure := catch:
      with-timeout write-timeout_:
        writing_.do: writer_.write bytes
    if failure: throw (ConnectionError "WRITE_FAILED" failure)

  close -> none:
    link.close
