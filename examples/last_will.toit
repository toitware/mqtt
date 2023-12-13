// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import mqtt.transport
import mqtt.packets

LAST-WILL-TOPIC ::= "toit/last-will-$(random)"
HOST ::= "test.mosquitto.org"

start-will-listener:
  client := mqtt.Client --host=HOST
  client.start
  client.subscribe LAST-WILL-TOPIC:: | topic msg |
    print "Received $msg.to-string"
    client.close

main:
  task:: start-will-listener

  last-will := mqtt.LastWill
    LAST-WILL-TOPIC
    "Bye!".to-byte-array
    --qos=1

  client := mqtt.Client --host=HOST

  options := mqtt.SessionOptions
      --client-id=""  // A fresh ID chosen by the broker.
      --last-will=last-will

  client.start --options=options

  print "Connected to broker"
  // Close without sending a disconnect packet.
  client.close --force
