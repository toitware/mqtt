// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

/**
An example demonstrating how to publish messages.

Works great with the subscribe examples.

Be default uses an MQTT broker on localhost.
*/

import mqtt
import net
import monitor

// You can also switch to "test.mosquitto.org", but be aware that
// all users share the same broker instance, and you should then also
// change the client id, as well as the topic.
HOST ::= "127.0.0.1"

CLIENT_ID ::= "toit-publish"
TOPIC ::= "toit/example/publish_subscribe"

main:
  transport := mqtt.TcpTransport net.open --host=HOST

  client := mqtt.Client --transport=transport
  client.start --client_id=CLIENT_ID
      --on_error=:: print "Client error: $it"

  print "Connected to broker"

  4.repeat:
    client.publish TOPIC "$it".to_byte_array
    print "Published '$it'"
    sleep --ms=1000

  client.close
