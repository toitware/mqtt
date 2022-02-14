// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net
import encoding.json

HOST ::= "test.mosquitto.org"
PORT ::= 1884

CLIENT_ID ::= "toit-username-password"
USERNAME ::= "wo"
PASSWORD ::= "writeonly"

main:
  socket := net.open.tcp_connect HOST PORT

  client := mqtt.Client
      --username=USERNAME
      --password=PASSWORD
      CLIENT_ID
      mqtt.TcpTransport socket

  print "connected to broker"

  payload := json.encode {
    "key": "value"
  }
  client.publish "a/b" payload

  print "successfully published"
