// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker-internal
import .broker-mosquitto
import .packet-test-client
import .transport

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  if test-with-mosquitto: return

  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  with-internal-broker --logger=logger run-test

/**
Tests that the client continues to reconnect if the transport reconnect fails.
*/
test create-transport/Lambda --logger/log.Logger:
  failing-transport /CallbackTestTransport? := null

  create-failing-transport := ::
    transport := create-transport.call
    failing-transport = CallbackTestTransport transport
    failing-transport

  reconnection-strategy := mqtt.DefaultSessionReconnectionStrategy
      --logger=logger.with-name "mqtt.reconnection_strategy"
      --attempt-delays=[
        Duration.ZERO,
        Duration.ZERO,
        Duration.ZERO,
      ]

  with-packet-client create-failing-transport
      --client-id = "disconnect-client1"
      --no-clean-session
      --reconnection-strategy = reconnection-strategy
      --logger=logger: | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    is-destroyed := false

    reconnect-attempt := 0
    reconnect-was-attempted := monitor.Latch
    failing-transport.on-reconnect = ::
      reconnect-attempt++
      if reconnect-attempt == 0:
        null
      else if reconnect-attempt <= 2:
        throw "RECONNECTION FAILING"
      // Finally it connects again.
      is-destroyed = false
      null

    failing-transport.on-write = ::
      if is-destroyed: throw "destroyed transport"

    wait-for-idle.call
    clear.call

    // Destroy the transport. From the client's side it looks as if all writes fail from now on.
    is-destroyed = true

    write-failed-latch := monitor.Latch

    // This packet will make it through after several reconnection attempts.
    client.publish "failing" #[] --qos=0

    expect reconnect-attempt > 2
