// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
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

main:
  socket := net.open.tcp_connect HOST PORT

  task:: start_will_listener

  last_will := mqtt.LastWillConfig.from_string
    LAST_WILL_TOPIC
    "Bye!"
    --qos=1

  client := mqtt.Client
    "toit-client-id"
    mqtt.TcpTransport socket
    --last_will=last_will
    --keep_alive=Duration --s=10

  print "connected to broker"

  client.close
