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


/**
A simple test target for a logger.

The keys and values are ignored.
*/
class TestLogTarget implements target.Target:
  messages /Map ::= {:}  // From level to list of messages.

  log level/int message/string names/List? keys/List? values/List? -> none:
    print "Logging $message (level = $level)"
    (messages.get level --init=:[]).add message
