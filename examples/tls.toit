// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net
import tls
import net.x509

main:
  socket := net.open.tcp_connect "127.0.0.1" 8883

  tls := tls.Socket.client socket
    --root_certificates=[SERVER_CERT]

  tls.handshake

  client := mqtt.Client
    "toit-client-id"
    mqtt.TcpTransport tls

  print "connected to broker"

SERVER_CERT := x509.Certificate.parse """\
-----BEGIN CERTIFICATE-----

<- insert server cert ->

-----END CERTIFICATE-----
"""
