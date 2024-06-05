// Copyright (C) 2024 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt

import .broker-internal
import .transport

test create-transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  client := mqtt.Client --transport=transport --logger=logger
  options := mqtt.SessionOptions --client-id="test_pubsub"
  client.start --options=options

  done := monitor.Latch

  foo-sharp-counter := 0
  longer-topic-counter := 0
  client.subscribe "done":: done.set true
  client.publish "done" "done" --qos=0
  done.get
  client.close

main args:
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level
  with-internal-broker --logger=logger: | create-transport/Lambda |
    test create-transport --logger=logger
