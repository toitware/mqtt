// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import monitor
import mqtt
import mqtt.packets as mqtt
import net

/**
An example demonstrating the full client.

The full client requires more steps but is more flexible.
*/


HOST ::= "test.mosquitto.org"
PORT ::= 1883

TOPIC_PREFIX ::= "toit/topic-$(random)"
CLIENT_ID ::= "toit-client-id-$(random)"

main:
  transport := mqtt.TcpTransport net.open --host=HOST --port=PORT
  client := mqtt.FullClient --transport=transport

  options := mqtt.SessionOptions --client_id=CLIENT_ID
  client.connect --options=options

  unsubscribed_latch := monitor.Latch

  task::
    client.handle: | packet /mqtt.Packet |
      // Send an ack back (for the packets that need it).
      // One should send acks as soon as possible, especially, if handling
      //   the packet could take time.
      // Note that the client may not send the ack if it is already closed.
      client.ack packet
      if packet is mqtt.PublishPacket:
        publish := packet as mqtt.PublishPacket
        print "Incoming: $publish.topic $publish.payload.to_string"
      else if packet is mqtt.SubAckPacket:
        print "Subscribed"
      else if packet is mqtt.UnsubAckPacket:
        unsubscribed_latch.set true
        print "Unsubscribed"
      else:
        print "unknown packet of type $packet.type"

  // Wait for the client to be ready.
  client.when_running: null

  client.subscribe "$TOPIC_PREFIX/#"
  client.publish "$TOPIC_PREFIX/foo" "hello_world".to_byte_array
  client.publish "$TOPIC_PREFIX/bar" "hello_world".to_byte_array --qos=1
  client.unsubscribe "$TOPIC_PREFIX/#"

  // Wait for the confirmation that we have unsubscribed.
  unsubscribed_latch.get
  client.close
