// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net
import monitor

main:
  socket := net.open.tcp_connect "127.0.0.1" 1883

  client := mqtt.Client
    --client_id = "toit-publish"
    --transport = mqtt.TcpTransport socket

  client.start --detached
  print "connected to broker"

  10.repeat:
    client.publish "toit/example/publish_subscribe" "$it".to_byte_array
    print "published '$it'"
    sleep --ms=1000

  client.close
