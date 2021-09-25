// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

main:
  socket := net.open.tcp_connect "127.0.0.1" 1883

  client := mqtt.Client
    "toit-publish"
    mqtt.TcpTransport socket

  print "connected to broker"

  10.repeat:
    sleep --ms=1000
    client.publish "a/b" "$it".to_byte_array
    print "published '$it'"

  client.close
