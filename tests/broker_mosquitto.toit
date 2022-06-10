// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import host.pipe
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import net


start_mosquitto:
  port /string := pipe.backticks "python" "third_party/ephemeral-port-reserve/ephemeral_port_reserve.py"
  port = port.trim
  fork_data := pipe.fork
      true  // use_path.
      pipe.PIPE_INHERITED  // stdin.
      pipe.PIPE_CREATED  // stdout.
      pipe.PIPE_CREATED  // stderr.
      "mosquitto"  // Program.
      ["mosquitto", "-v", "-p", port]  // Args.
  return [
    int.parse port,
    fork_data
  ]

with_mosquitto --logger/log.Logger [block]:
  mosquitto_data := start_mosquitto
  port := mosquitto_data[0]
  logger.info "started mosquitto on port $port"

  mosquitto_fork_data := mosquitto_data[1]

  mosquitto_is_running := monitor.Latch
  stdout_bytes := #[]
  stderr_bytes := #[]
  task::
    stdout /pipe.OpenPipe := mosquitto_fork_data[1]
    while chunk := stdout.read:
      logger.debug chunk.to_string.trim
      stdout_bytes += chunk
  task::
    stderr /pipe.OpenPipe := mosquitto_fork_data[2]
    while chunk := stderr.read:
      str := chunk.to_string.trim
      logger.debug str
      stderr_bytes += chunk
      if str.contains "mosquitto version" and str.contains "running":
        mosquitto_is_running.set true

  mosquitto_is_running.get

  network := net.open

  try:
    block.call:: mqtt.TcpTransport network --host="localhost" --port=port
  finally: | is_exception _ |
    pid := mosquitto_fork_data[3]
    logger.info "Killing mosquitto server"
    pipe.kill_ pid 15
    pipe.wait_for pid
    if is_exception:
      print stdout_bytes.to_string
      print stderr_bytes.to_string

/**
Function to test with an external mosquitto.
Can sometimes be useful, as the logging is better.
*/
with_external_mosquitto --logger/log.Logger [block]:
  network := net.open
  block.call:: mqtt.TcpTransport network --host="localhost" --port=1883
