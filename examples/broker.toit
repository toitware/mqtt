// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import net
import mqtt
import mqtt.broker as server

main:
  network := net.open
  broker := server.Broker (mqtt.TcpListener (network.tcp-listen 1883))
      --limits=(server.BrokerLimits --connections=4 --payload-bytes=32_768)
  try:
    broker.start
    broker.wait-closed
  finally:
    broker.close
    broker.wait-closed
    network.close
