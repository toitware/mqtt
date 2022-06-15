// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

/**
Demonstrates the use of Adafruit's MQTT server.

Adafruit is free, but has some limitations. For example, one can only publish
  at most 30 messages per minute.

Before running this program:
- create an account at https://io.adafruit.com/
- create a feed: https://io.adafruit.com/floitsch_toit/feeds
- update the $ADAFRUIT_IO_USERNAME, $ADAFRUIT_IO_KEY, and $ADAFRUIT_IO_FEEDNAME constants.
*/

import mqtt
import net
import certificate_roots

ADAFRUIT_IO_USERNAME ::= "<YOUR_USERNAME>"
// From io.adafruit.com/$ADAFRUIT_IO_USERNAME/dashboards -> click "My Key"
ADAFRUIT_IO_KEY ::= "<YOUR_KEY>"

ADAFRUIT_IO_FEEDNAME ::= "<YOUR_FEEDNAME>"

HOST ::= "io.adafruit.com"

main:
  network := net.open

  transport := mqtt.TcpTransport.tls network --host=HOST
      --root_certificates=[ certificate_roots.DIGICERT_GLOBAL_ROOT_CA ]
  /**
  // Alternatively, you can also connect without TLS, by using the
  // following transport:
  transport := mqtt.TcpTransport network --host=HOST
  // In that case you can remove the `certificate_roots` import.
  */

  client := mqtt.Client --transport=transport

  options := mqtt.SessionOptions
    --client_id = "toit-example-client"
    --username = ADAFRUIT_IO_USERNAME
    --password = ADAFRUIT_IO_KEY

  client.start --options=options

  print "connected to broker"

  topic := "$ADAFRUIT_IO_USERNAME/feeds/$ADAFRUIT_IO_FEEDNAME"

  // Simulates a temperature sensor.
  temperature := 25.0
  10.repeat:
    temperature += ((random 100) - 50) / 100.0
    client.publish topic "$temperature".to_byte_array
    // Don't publish too often to avoid rate limiting.
    sleep --ms=2_500

  client.close
