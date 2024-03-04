// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import host.pipe
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import net

get-mosquitto-version:
  fork-data := pipe.fork
      true  // use_path.
      pipe.PIPE-INHERITED  // stdin.
      pipe.PIPE-CREATED    // stdout.
      pipe.PIPE-INHERITED  // stderr.
      "mosquitto"  // Program.
      ["mosquitto", "-h"]  // Args.
  stdout /pipe.OpenPipe := fork-data[1]
  out-data := #[]
  task::
    while chunk := stdout.read:
      out-data += chunk

  pipe.wait-for fork-data[3]
  out-str := out-data.to-string
  first-line /string := (out-str.split "\n").first
  return first-line.trim --left "mosquitto version "

start-mosquitto:
  port /string := pipe.backticks "python" "third_party/ephemeral-port-reserve/ephemeral_port_reserve.py"
  port = port.trim
  fork-data := pipe.fork
      true  // use_path.
      pipe.PIPE-INHERITED  // stdin.
      pipe.PIPE-CREATED  // stdout.
      pipe.PIPE-CREATED  // stderr.
      "mosquitto"  // Program.
      ["mosquitto", "-v", "-p", port]  // Args.
  return [
    int.parse port,
    fork-data
  ]

with-mosquitto --logger/log.Logger [block]:
  mosquitto-data := start-mosquitto
  port := mosquitto-data[0]
  logger.info "started mosquitto on port $port"

  mosquitto-fork-data := mosquitto-data[1]

  mosquitto-is-running := monitor.Latch
  stdout-bytes := #[]
  stderr-bytes := #[]
  task::
    stdout /pipe.OpenPipe := mosquitto-fork-data[1]
    while chunk := stdout.read:
      logger.debug chunk.to-string.trim
      stdout-bytes += chunk
  task::
    stderr /pipe.OpenPipe := mosquitto-fork-data[2]
    while chunk := stderr.read:
      str := chunk.to-string.trim
      logger.debug str
      stderr-bytes += chunk
      full-str := stderr-bytes.to-string
      if full-str.contains "Opening ipv6 listen socket on port":
        mosquitto-is-running.set true

  // Give mosquitto a second to start.
  // If it didn't start we might be looking for the wrong line in its output.
  // There was a change between 1.6.9 and 2.0.14. Could be that there is
  // going to be another one.
  with-timeout --ms=1_000:
    mosquitto-is-running.get

  network := net.open

  // Even though Mosquitto claims that it is listening (and in v2 it even claims
  // that it is "running"), that doesn't mean that it is ready for connections
  // yet.
  for i := 0; i < 10; i++:
    socket := null
    exception := catch:
      socket = network.tcp-connect "localhost" port
    if socket:
      socket.close
      break
    sleep --ms=(50*i)

  try:
    block.call:: mqtt.TcpTransport --net-open=(:: net.open) --host="localhost" --port=port
  finally: | is-exception _ |
    pid := mosquitto-fork-data[3]
    logger.info "killing mosquitto server"
    pipe.kill_ pid 15
    pipe.wait-for pid
    if is-exception:
      print stdout-bytes.to-string
      print stderr-bytes.to-string

/**
Function to test with an external mosquitto.
Can sometimes be useful, as the logging is better.
*/
with-external-mosquitto --logger/log.Logger [block]:
  network := net.open
  block.call:: mqtt.TcpTransport --net-open=(:: net.open) --host="localhost" --port=1883
