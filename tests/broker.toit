// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple MQTT broker library.

This implementation is intended to be used for testing.
*/

import reader
import writer
import mqtt.packets as mqtt
import log
import .log

interface Transport implements reader.Reader:
  write bytes/ByteArray -> int
  read -> ByteArray?
  close -> none

interface ServerTransport:
  listen callback/Lambda -> none
  close -> none

class Connection_:
  transport_ /Transport
  reader_ /reader.BufferedReader
  writer_ /writer.Writer

  constructor .transport_:
    reader_ = reader.BufferedReader transport_
    writer_ = writer.Writer transport_

  read -> mqtt.Packet?:
    print "deserializing"
    return mqtt.Packet.deserialize reader_

  write packet/mqtt.Packet:
    writer_.write packet.serialize

  close -> none:
    transport_.close

class Subscriptions:

class Session:
  client_id_ /string
  logger_ /log.Logger
  connection_ /Connection_? := null

  constructor .client_id_ --logger/log.Logger:
    logger_ = logger

  run connection/Connection_:
    if connection_:
      connection_.close
    connection_ = connection
    try:
      while packet := connection.read:
        logger_.info "received $(stringify_packet packet) from client $client_id_"
        handle packet
      logger_.info "client $client_id_ disconnected"
    finally:
      connection_ = null

  handle packet/mqtt.Packet:
    if packet is mqtt.SubscribePacket:
      subscribe_packet := packet as mqtt.SubscribePacket
      subscribe subscribe_packet

  subscribe packet/mqtt.SubscribePacket:


class Broker:
  sessions_ /Map ::= {:}
  server_transport_ /ServerTransport
  logger_ /log.Logger

  constructor .server_transport_ --logger/log.Logger=log.default:
    logger_ = logger

  start:
    logger_.info "starting broker"
    server_transport_.listen::
      logger_.info "listening"
      connection := Connection_ it
      exception := catch --trace:
        print "reading connection"
        packet := connection.read
        logger_.info "Read packet $(stringify_packet packet)"
        if packet is not mqtt.ConnectPacket:
          logger_.error "didn't receive connect packet, but got packet of type $packet.type"
          connection.close
          continue.listen

        connect := packet as mqtt.ConnectPacket

        logger_.info "new connection-request: $(stringify_packet connect)"

        client_id := connect.client_id
        connack /mqtt.ConnAckPacket ::= ?
        // We are not using the codes from the packet file, just to have a bit of
        // redundancy. We want to avoid bad copy/pasting.
        if client_id == "REFUSED-UNACCEPTABLE":
          logger_.info "refusing connection request because of unnacceptable protocol version"
          // We are not using the codes from the packet file, just to have a bit of
          // redundancy. We want to avoid bad copy/pasting.
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x01
        else if client_id == "REFUSED-IDENTIFIER-REJECTED":
          logger_.info "refusing connection request because of rejected identifier"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x02
        else if client_id == "REFUSED-SERVER-UNAVAILABLE":
          logger_.info "refusing connection request because of unavailable server"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x03
        else if client_id == "REFUSED-BAD-USERNAME-PASSWORD":
          logger_.info "refusing connection request because of bad username/password"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x04
        else if client_id == "REFUSED-NOT-AUTHORIZED":
          logger_.info "refusing connection request because of not authorized"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x05
        else:
          if client_id == "": client_id = "unknown-$(random)"
          session_present /bool ::= ?
          session /Session? := connect.clean_session ? null : sessions_.get connect.client_id
          if session:
            logger_.info "existing session for client $client_id"
            session_present = true
          else:
            logger_.info "new session for client $client_id"
            session = Session client_id --logger=logger_
            sessions_[connect.client_id] = session
            session_present = false

          task:: session.run connection
          connack = mqtt.ConnAckPacket --session_present=session_present --return_code=0x00

        connection.write connack
        if connack.return_code != 0:
          connection.close
        else:

