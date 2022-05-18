// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

main:
  socket := net.open.tcp_connect "127.0.0.1" 1883

  client := mqtt.Client
    "toit-subscribe"
    mqtt.TcpTransport socket

  print "connected to broker"

  client.subscribe [
    mqtt.TopicFilter "a/b" --qos=1,
    mqtt.TopicFilter "b/c" --qos=1,
    mqtt.TopicFilter "c/d" --qos=1,
  ]

  client.handle: | topic payload |
    print "$topic: $payload.to_string_non_throwing"
