// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import log
import monitor
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt

import .transport

is_idle_packet_ activity_entry/List idle_topic/string -> bool:
  if activity_entry[0] != "read" and activity_entry[0] != "write": return false
  if activity_entry[1] is not mqtt.PublishPacket: return false
  publish := activity_entry[1] as mqtt.PublishPacket
  return publish.topic == idle_topic

filter_idle_packets_ activity/List idle_topic/string -> List:
  return activity.filter: not is_idle_packet_ it idle_topic

/**
Tests that the client and broker correctly ack packets.

Calls the block with a client, and three lambdas:
- wait_for_idle: sends a packet to the broker and waits for it to come back.
- clear: clears the activity that is logged.
- get_activity: returns the packets that were captured (see $TestTransport).
*/
with_packet_client create_transport/Lambda [block]
    --logger /log.Logger
    --client_id/string = "test-client"
    --clean_session /bool = true
    --keep_alive /Duration = (Duration --s=10_000) // Mosquitto doesn't support 0-duration keep-alives.
    --reconnection_strategy /mqtt.ReconnectionStrategy? = null
    --persistence_store /mqtt.PersistenceStore? = null
    --on_handle_error /Lambda? = null
    --read_filter /Lambda? = null
    --write_filter /Lambda? = null:
  transport /mqtt.Transport := create_transport.call
  logging_transport := TestTransport transport --read_filter=read_filter --write_filter=write_filter
  client := mqtt.FullClient --transport=logging_transport --logger=logger

  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  options := mqtt.SessionOptions
      --client_id = client_id
      --keep_alive = keep_alive
      --clean_session = clean_session
  client.connect --options=options
      --persistence_store = persistence_store
      --reconnection_strategy = reconnection_strategy

  // We are going to use a "idle" ping packet to know when the broker is idle.
  // It's not a guarantee as the broker is allowed to send acks whenever it wants, but
  // it should be quite stable.
  idle := monitor.Semaphore

  idle_topic := "idle-$client_id-$random"
  wait_for_idle := ::
    client.publish idle_topic #[] --qos=0
    idle.down
    client.publish idle_topic #[] --qos=0
    idle.down

  task::
    exception := catch --unwind=(on_handle_error == null):
      client.handle: | packet/mqtt.Packet |
        logger.info "received $(mqtt.Packet.debug_string_ packet)"
        if packet is mqtt.PublishPacket:
          client.ack packet
          if (packet as mqtt.PublishPacket).topic == idle_topic: idle.up

      logger.info "client shut down"
    if exception:
      on_handle_error.call exception

  client.when_running:
    client.subscribe idle_topic --max_qos=0

  block.call client
      wait_for_idle
      :: logging_transport.clear
      :: filter_idle_packets_ logging_transport.activity idle_topic

  client.close
