// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import .broker as broker
import mqtt
import .log
import log
import .transport

main:
  server_transport := TestServerTransport
  logger := log.Logger log.DEBUG_LEVEL TestLogTarget --name="client test"
  broker := broker.Broker server_transport --logger=logger
  client_transport := TestClientTransport server_transport
  client := mqtt.Client --transport=client_transport --logger=logger

  task:: broker.start

  options := mqtt.SessionOptions --client_id="test_client"
  client.connect --options=options
  client.handle:
    print "received packet $(stringify_packet it)"
