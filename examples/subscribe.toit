// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

TOPIC ::= "toit/example/publish_subscribe"
main:
  socket := net.open.tcp_connect "127.0.0.1" 1883

  client := mqtt.Client
    --client_id = "toit-subscribe"
    --transport = mqtt.TcpTransport socket

  client.start --detached
  print "connected to broker"

  client.subscribe TOPIC --max_qos=1:: | topic payload |
    print "$topic: $payload.to_string_non_throwing"
