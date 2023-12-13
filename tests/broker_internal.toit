// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import log
import mqtt.broker
import .transport

with-internal-broker --logger/log.Logger [block]:
  server-transport := TestServerTransport
  broker := broker.Broker server-transport --logger=logger
  broker-task := task:: broker.start

  try:
    block.call:: TestClientTransport server-transport
  finally:
    broker-task.cancel

