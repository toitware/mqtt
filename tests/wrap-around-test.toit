// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import net

import .broker-internal
import .broker-mosquitto
import .transport

test create-transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  client := mqtt.Client --transport=transport --logger=logger
  options := mqtt.SessionOptions --client-id="test_wrap_around"
  client.start --options=options

  client.client_.session_.next-packet-id_ = 0xFFFA

  done := monitor.Latch

  foo-counter := 0
  client.subscribe "foo":: foo-counter++
  client.subscribe "done":: done.set true

  iterations := 20
  iterations.repeat:
    client.publish "foo" "msg" --qos=1

  client.publish "done" "done".to-byte-array --qos=0
  done.get

  expect-equals 20 iterations
  client.close

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
