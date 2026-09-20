// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import mqtt

main:
  client := mqtt.Client --connector=(mqtt.TcpConnector --host="localhost")
      --options=(mqtt.SessionOptions --client-id="display" --clean-session)
  client.start
  try:
    (client.subscribe "sensor/#").wait
    while message := client.receive:
      print "$message.topic: $message.payload.to-string"
  finally:
    client.close --force
    client.wait-closed
