// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import mqtt.transport
import mqtt.packets
import net

LAST_WILL_TOPIC ::= "/toit-last-will"
HOST ::= "test.mosquitto.org"
PORT ::= 1883

start_will_listener:
  socket := net.open.tcp_connect HOST PORT
  client := mqtt.Client "toit-client-id2"
      mqtt.TcpTransport socket
  client.subscribe LAST_WILL_TOPIC --qos=1
  client.handle: | topic msg |
    print "Received $topic $msg.to_string"
    client.close
    return

class HoleTcpTransport implements transport.Transport:
  transport_ / transport.Transport
  forward_messages := true

  constructor .transport_:

  send packet/packets.Packet:
    if forward_messages: transport_.send packet

  receive --timeout/Duration?=null -> packets.Packet?:
    return transport_.receive --timeout=timeout

main:
  socket := net.open.tcp_connect HOST PORT

  task:: start_will_listener

  last_will := mqtt.LastWill
    LAST_WILL_TOPIC
    "Bye!".to_byte_array
    --qos=1

  tcp_transport := mqtt.TcpTransport socket
  hole_transport := HoleTcpTransport tcp_transport

  client := mqtt.Client
    "toit-client-id"
    hole_transport
    --last_will=last_will
    --keep_alive=Duration --s=10

  print "connected to broker"

  // Disconnect the sending part of the client, so that the broker
  // thinks that the device dropped off.
  hole_transport.forward_messages = false
