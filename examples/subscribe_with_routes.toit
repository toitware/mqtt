// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

/**
An example demonstrating how to subscribe to messages.
The example provides routes to the client, so that it can establish
  them when starting up.

Works great with the publish example.

Be default uses an MQTT broker on localhost.
*/

// You can also switch to "test.mosquitto.org", but be aware that
// all users share the same broker instance, and you should then also
// change the client id, as well as the topic.
HOST ::= "127.0.0.1"

CLIENT_ID ::= "toit-subscribe-$(random)"
TOPIC ::= "toit/example/#"

main:
  transport := mqtt.TcpTransport net.open --host=HOST

  client := mqtt.Client --transport=transport --routes={
    TOPIC: :: | topic payload |
      print "Received: $topic: $payload.to_string_non_throwing"
  }

  client.start --client_id=CLIENT_ID
      --on_error=:: print "Client error: $it"
