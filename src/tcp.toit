// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import net.tcp
import writer
import reader

import .transport
import .packets

/**
A transport for backing a MQTT client with TCP or TLS/TCP.
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
    writer_.write
      packet.serialize

  receive -> Packet:
    return Packet.deserialize reader_
