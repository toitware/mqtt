// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

main:
  socket := net.open.tcp_connect "127.0.0.1" 1883

  client := mqtt.Client
    "toit-subscribe"
    mqtt.TcpTransport socket

  print "connected to broker"

  client.subscribe [
    mqtt.TopicFilter "a/b" --qos=1,
    mqtt.TopicFilter "b/c" --qos=1,
    mqtt.TopicFilter "c/d" --qos=1,
  ]

  client.subscribe "d/e" --qos=1

  task::
    5.repeat:
      client.publish "a/b" "a/b $it".to_byte_array
      client.publish "b/c" "b/c $it".to_byte_array
      client.publish "c/d" "c/d $it".to_byte_array
      client.publish "d/e" "d/e $it".to_byte_array
      sleep --ms=200
    sleep --ms=1000
    client.close

  received_count := 0
  client.handle: | topic payload |
    print "$topic: $payload.to_string_non_throwing"
    received_count++
    if received_count == 10:
      client.unsubscribe "a/b"
    if received_count == 15:
      client.unsubscribe_all [ "b/c", "c/d" ]
