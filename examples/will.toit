// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

main:
  socket := net.open.tcp_connect "test.mosquitto.org" 1883

  will := mqtt.WillConfig
    "/will-topic"
    "Bye!"
    --retain=true
    --qos=1

  client := mqtt.Client
    "toit-client-id"
    mqtt.TcpTransport socket
    --will=will

  print "connected to broker"
