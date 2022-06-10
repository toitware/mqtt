// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .full_client // For toitdoc.

/**
A last will.

When a client disconnects ungracefully from the broker, the broker sends this message.
*/
class LastWill:
  retain/bool
  qos/int
  topic/string
  payload/ByteArray

  /**
  Constructs the configuration of a last-will message.

  The parameters $topic, $payload, $qos and $retain have the same
    meaning as for $FullClient.publish, and are used when the last-will message
    is eventually sent.
  */
  constructor .topic .payload --.qos --.retain=false:
    if not 0 <= qos <= 2: throw "INVALID_QOS"
