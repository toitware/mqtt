// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net
import tls
import net.x509

HOST ::= "127.0.0.1"
PORT ::= 8883

CLIENT_ID ::= "toit-client-id"

main:
  transport := mqtt.TcpTransport.tls net.open --host=HOST --port=PORT
    --root_certificates=[SERVER_CERTIFICATE]

  client := mqtt.Client --transport=transport

  client.start --client_id=CLIENT_ID
  print "Connected to broker"


SERVER_CERTIFICATE := x509.Certificate.parse """\
-----BEGIN CERTIFICATE-----

<- insert server cert ->

-----END CERTIFICATE-----
"""
