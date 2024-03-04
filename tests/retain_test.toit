// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker-internal
import .broker-mosquitto
import .packet-test-client
import .transport

/**
Tests that the client and broker correctly ack packets.
*/
test create-transport/Lambda --logger/log.Logger:
  topic := "test/retain"
  with-packet-client create-transport
      --logger=logger : | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    client.subscribe topic
    client.publish topic "test".to-byte-array
    wait-for-idle.call

    clear.call
    client.subscribe topic
    wait-for-idle.call

    // 2 messages for the subscription.
    expect-equals 2 get-activity.call.size

    2.repeat: | qos |
      // Now send a packet with retain.
      // We then subscribe again to the same topic.
      // This time, we should receive the same packet, but with a 'retain' flag set.
      client.publish topic "test".to-byte-array --retain --qos=qos
      wait-for-idle.call

      clear.call
      client.subscribe topic
      wait-for-idle.call

      activity := get-activity.call
      // 2 messages for the subscription.
      // 1 or 2 for the retained packet. (depending on qos)
      expected-count := qos == 0 ? 3 : 4
      expect-equals expected-count activity.size
      reads := activity.filter: it[0] == "read"
      writes := activity.filter: it[0] == "write"
      publish := (reads.filter: it[1] is mqtt.PublishPacket)[0][1]
      expect-equals topic publish.topic
      if qos == 0: expect-null publish.packet-id
      else: expect-not-null publish.packet-id
      expect publish.retain

    // A message with 0 bytes drops the retained packet.
    client.publish topic #[] --retain
    wait-for-idle.call

    clear.call
    client.subscribe topic
    wait-for-idle.call

    // 2 messages for the subscription.
    // No retained packet.
    expect-equals 2 get-activity.call.size

    // Check that other clients also get the retained message.

    client.publish topic "available for other clients".to-byte-array --qos=0 --retain

    with-packet-client create-transport
        --client-id = "other client"
        --logger = logger:
      | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

      clear.call
      client.subscribe topic
      wait-for-idle.call

      activity := get-activity.call
      reads := activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
      retained /mqtt.PublishPacket := reads.first[1]
      expect-equals topic retained.topic
      expect-equals "available for other clients" retained.payload.to-string
      expect retained.retain

  // The retained message stays even when the original sender has died.
  with-packet-client create-transport
      --client-id = "third client"
      --logger = logger:
    | client/mqtt.FullClient wait-for-idle/Lambda clear/Lambda get-activity/Lambda |

    clear.call
    client.subscribe topic
    wait-for-idle.call

    activity := get-activity.call
    reads := activity.filter: it[0] == "read" and it[1] is mqtt.PublishPacket
    retained /mqtt.PublishPacket := reads.first[1]
    expect-equals topic retained.topic
    expect-equals "available for other clients" retained.payload.to-string
    expect retained.retain


main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
