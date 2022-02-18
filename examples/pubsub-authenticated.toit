// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net
import encoding.json
import tls
import net.x509
import certificate_roots

HOST ::= "test.mosquitto.org"
PORT ::= 8886
SERVER_CERTIFICATE ::= certificate_roots.ISRG_ROOT_X1

USERNAME ::= "rw"
PASSWORD ::= "readwrite"

CLIENT_ID ::= "toit-pubsub"

main:
  socket := net.open.tcp_connect HOST PORT

  tls := tls.Socket.client socket
    --root_certificates=[SERVER_CERTIFICATE]

  client := mqtt.Client
      CLIENT_ID
      mqtt.TcpTransport tls
      --username=USERNAME
      --password=PASSWORD

  print "connected to broker"

  task::
    print "In task"
    subscribe client

  10.repeat:
    payload := json.encode {
      "key": it
    }
    channel := ?
    if it % 2 == 0: channel = "topic/foo"
    else: channel = "topic/bar"
    client.publish channel payload
    sleep --ms=1000

  // Send a signal to stop listening for messages.
  client.publish "topic/foo" #[0]

  print "done"

subscribe client/mqtt.Client:
  client.subscribe "topic/foo" --qos=1
  client.subscribe "topic/bar" --qos=1

  client.handle: | topic payload |
    if payload == #[0]:
      // Received the token to stop listening.
      return
    print "$topic: $payload.to_string_non_throwing"
