// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import mqtt.transport
import mqtt.packets
import net

LAST_WILL_TOPIC ::= "toit/last-will-$(random)"
HOST ::= "test.mosquitto.org"
PORT ::= 1883

start_will_listener:
  transport := mqtt.TcpTransport net.open --host=HOST --port=PORT
  client := mqtt.Client --transport=transport
  client.start
  client.subscribe LAST_WILL_TOPIC:: | topic msg |
    print "Received $msg.to_string"
    client.close

main:
  task:: start_will_listener

  last_will := mqtt.LastWill
    LAST_WILL_TOPIC
    "Bye!".to_byte_array
    --qos=1

  transport := mqtt.TcpTransport net.open --host=HOST --port=PORT

  client := mqtt.Client --transport=transport

  options := mqtt.SessionOptions
      --client_id=""  // A fresh ID chosen by the broker.
      --last_will=last_will

  client.start --options=options

  print "Connected to broker"
  // Close without sending a disconnect packet.
  client.close --force
