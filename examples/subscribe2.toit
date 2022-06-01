// Copyright (C) 2021 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import mqtt
import net

TOPIC ::= "toit/example/#"
main:
  transport := mqtt.TcpTransport net.open --host="127.0.0.1"
  // socket := net.open.tcp_connect "127.0.0.1" 1883
  // transport := mqtt.TcpTransport socket

  router := mqtt.Router --transport=transport
  options := mqtt.SessionOptions --client_id="toit-subscribe"
  router.start --detached --session_options=options

  print "connected to broker"

//  router.subscribe "toit/example/a" --max_qos=1:: | topic payload |
//    print "$topic: $payload.to_string_non_throwing"
  router.subscribe "toit/+/a" --max_qos=0:: | topic payload |
    print "$topic: $payload.to_string_non_throwing"
  router.subscribe TOPIC --max_qos=1:: | topic payload |
    print "$topic: $payload.to_string_non_throwing"

  router.publish "toit/example/a" "hello world".to_byte_array --qos=1