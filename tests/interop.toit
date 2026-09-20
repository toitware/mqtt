// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import host.pipe
import net
import mqtt
import mqtt.broker as server

client-with-mosquitto:
  network := net.open
  reservation := network.tcp-listen 0
  port := reservation.local-address.port
  reservation.close
  network.close
  process := pipe.fork --use-path "mosquitto" ["mosquitto", "-p", "$port"]
  client := mqtt.Client --connector=(mqtt.TcpConnector --host="127.0.0.1" --port=port)
      --options=(mqtt.SessionOptions --client-id="interop" --clean-session)
      --retry=(mqtt.RetryPolicy --initial-delay=(Duration --ms=20) --maximum-delay=(Duration --ms=100))
  client.start
  try:
    with-timeout --ms=10_000:
      client.wait-connected
      (client.publish "state" "ready" --retain).wait
      expect-equals 1 (client.subscribe "state").wait
      message := client.receive
      expect-equals "ready" message.payload.to-string
      expect message.retain
      (client.unsubscribe "state").wait
      (client.publish "state" #[] --retain).wait
      client.close
      client.wait-closed
  finally:
    critical-do:
      client.close --force
      catch: client.wait-closed
      pipe.kill_ process.pid 15
      process.wait

broker-with-mosquitto-clients:
  network := net.open
  socket := network.tcp-listen 0
  port := socket.local-address.port
  broker := server.Broker (mqtt.TcpListener socket)
  broker.start
  try:
    with-timeout --ms=10_000:
      pipe.backticks ["mosquitto_pub", "-V", "mqttv311", "-h", "127.0.0.1", "-p", "$port",
          "-t", "sensor/temperature", "-m", "42", "-q", "1", "-r"]
      value := pipe.backticks ["mosquitto_sub", "-V", "mqttv311", "-h", "127.0.0.1", "-p", "$port",
          "-t", "sensor/#", "-q", "1", "-C", "1", "-W", "5"]
      expect-equals "42" value.trim
  finally:
    critical-do:
      broker.close
      broker.wait-closed
      network.close

main:
  client-with-mosquitto
  broker-with-mosquitto-clients
