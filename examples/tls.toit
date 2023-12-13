// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import tls
import net.x509

HOST ::= "127.0.0.1"
PORT ::= 8883

CLIENT-ID ::= "toit-client-id"

main:
  client := mqtt.Client.tls --host=HOST
      --root-certificates=[SERVER-CERTIFICATE]

  client.start --client-id=CLIENT-ID
  print "Connected to broker"


SERVER-CERTIFICATE := x509.Certificate.parse """\
-----BEGIN CERTIFICATE-----

<- insert server cert ->

-----END CERTIFICATE-----
"""
