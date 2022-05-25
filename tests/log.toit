// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple logger for testing.
*/

import log
import log.target
import mqtt.packets as mqtt

stringify_packet packet/mqtt.Packet -> string:
  if packet is mqtt.ConnectPacket:
    connect := packet as mqtt.ConnectPacket
    "foo" + "bar"
    return ("Connect: $connect.client_id - "
            + "$(connect.clean_session ? "clean": "reuse") - "
            + "$(connect.last_will ? "last-will for $connect.last_will.topic": "no last will") - "
            + "$(connect.username ? "with username $connect.username": "no username") - "
            + "$(connect.password ? "with password $connect.password": "no password") - "
            + "$(connect.keep_alive)")
  else if packet is mqtt.PingReqPacket:
    return "Ping request"
  else if packet is mqtt.PingRespPacket:
    return "Ping response"
  else if packet is mqtt.ConnAckPacket:
    connack := packet as mqtt.ConnAckPacket
    return "ConnAck: $connack.return_code $connack.session_present"
  else if packet is mqtt.PublishPacket:
    publish := packet as mqtt.PublishPacket
    return "Publish($publish.packet_id) - "
        + "$publish.topic - "
        + "$publish.qos - "
        + "$(publish.duplicate ? "dup": "no dup") - "
        + "$(publish.retain ? "retain": "no retain") - "
        + "$(publish.payload.size) bytes"
  else:
    return "Packet of type $packet.type"

/**
A simple test target for a logger.

The keys and values are ignored.
*/
class TestLogTarget implements target.Target:
  messages /Map ::= {:}  // From level to list of messages.

  log level/int message/string names/List? keys/List? values/List? -> none:
    print "Logging $message (level = $level)"
    (messages.get level --init=:[]).add message
