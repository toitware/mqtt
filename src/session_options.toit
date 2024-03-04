// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .last-will

/**
Options to connect to an MQTT broker.
*/
class SessionOptions:
  static DEFAULT-KEEP-ALIVE ::= Duration --s=60
  static DEFAULT-MAX-INFLIGHT ::= 20

  client-id     /string
  clean-session /bool
  username      /string?
  password      /string?
  keep-alive    /Duration
  last-will     /LastWill?
  max-inflight  /int

  /**
  The $client-id (client identifier) will be used by the broker to identify a client.
    It should be unique per broker and can be between 1 and 23 characters long.
    Only characters and numbers are allowed

  If necessary, the $username/$password credentials can be used to authenticate.

  The $keep-alive informs the server of the maximum duration between two packets.
    The client automatically sends PINGREQ messages when necessary. If the value is
    lower, then the server detects disconnects faster, but the client needs to send
    more messages.
  If $keep-alive is set to 0, the broker does not disconnect due to inactivity, and
    the client won't send any ping requests.

  When provided, the $last-will configuration is used to send when the client
    disconnects ungracefully.

  The $max-inflight parameter sets the maximum number of non-acknowledged QoS 1 packets.
    If the client tries to send a QoS=1 packet while $max-inflight other packets are
    still waiting for an acknowledgement, then the client blocks the 'publish' call
    until the number of in-flight packets is below $max-inflight.
  */
  constructor
      --.client-id
      --.clean-session = false
      --.username = null
      --.password = null
      --.keep-alive = DEFAULT-KEEP-ALIVE
      --.last-will = null
      --.max-inflight = DEFAULT-MAX-INFLIGHT:
    if max-inflight < 1: throw "INVALID_ARGUMENT"
