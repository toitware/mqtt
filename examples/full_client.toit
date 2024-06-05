// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import monitor
import mqtt
import mqtt.packets as mqtt

/**
An example demonstrating the full client.

The full client requires more steps but is more flexible.
*/


HOST ::= "test.mosquitto.org"
PORT ::= 1883

TOPIC-PREFIX ::= "toit/topic-$(random)"
CLIENT-ID ::= "toit-client-id-$(random)"

main:
  transport := mqtt.TcpTransport --host=HOST --port=PORT
  client := mqtt.FullClient --transport=transport

  options := mqtt.SessionOptions --client-id=CLIENT-ID
  client.connect --options=options

  unsubscribed-latch := monitor.Latch

  task::
    client.handle: | packet /mqtt.Packet |
      // Send an ack back (for the packets that need it).
      // One should send acks as soon as possible, especially, if handling
      //   the packet could take time.
      // Note that the client may not send the ack if it is already closed.
      client.ack packet
      if packet is mqtt.PublishPacket:
        publish := packet as mqtt.PublishPacket
        print "Incoming: $publish.topic $publish.payload.to-string"
      else if packet is mqtt.SubAckPacket:
        print "Subscribed"
      else if packet is mqtt.UnsubAckPacket:
        unsubscribed-latch.set true
        print "Unsubscribed"
      else:
        print "Unknown packet of type $packet.type"

  // Wait for the client to be ready.
  // Note that the client can be used outside the `when_running` block, but
  // users should wait for the client to be ready before using it.
  // For example:
  // ```
  // client.when_running: null
  // client.subscribe ...
  // ```
  client.when-running:
    client.subscribe "$TOPIC-PREFIX/#"
    client.publish "$TOPIC-PREFIX/foo" "hello_world"
    client.publish "$TOPIC-PREFIX/bar" "hello_world" --qos=1
    client.unsubscribe "$TOPIC-PREFIX/#"

    // Wait for the confirmation that we have unsubscribed.
    unsubscribed-latch.get
    client.close
