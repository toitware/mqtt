// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import log
import mqtt.broker
import .transport

with_internal_broker --logger/log.Logger [block]:

  server_transport := TestServerTransport
  broker := broker.Broker server_transport --logger=logger
  broker_task := task:: broker.start

  try:
    block.call:: TestClientTransport server_transport
  finally:
    broker_task.cancel

