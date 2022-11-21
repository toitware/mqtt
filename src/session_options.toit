// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .last_will

/**
Options to connect to an MQTT broker.
*/
class SessionOptions:
  static DEFAULT_KEEP_ALIVE ::= Duration --s=60
  static DEFAULT_MAX_INFLIGHT ::= 20

  client_id     /string
  clean_session /bool
  username      /string?
  password      /string?
  keep_alive    /Duration
  last_will     /LastWill?
  max_inflight  /int

  /**
  The $client_id (client identifier) will be used by the broker to identify a client.
    It should be unique per broker and can be between 1 and 23 characters long.
    Only characters and numbers are allowed

  If necessary, the $username/$password credentials can be used to authenticate.

  The $keep_alive informs the server of the maximum duration between two packets.
    The client automatically sends PINGREQ messages when necessary. If the value is
    lower, then the server detects disconnects faster, but the client needs to send
    more messages.
  If $keep_alive is set to 0, the broker does not disconnect due to inactivity, and
    the client won't send any ping requests.

  When provided, the $last_will configuration is used to send when the client
    disconnects ungracefully.

  The $max_inflight parameter sets the maximum number of non-acknowledged QoS 1 packets.
    If the client tries to send a QoS=1 packet while $max_inflight other packets are
    still waiting for an acknowledgement, then the client blocks the 'publish' call
    until the number of in-flight packets is below $max_inflight.
  */
  constructor
      --.client_id
      --.clean_session = false
      --.username = null
      --.password = null
      --.keep_alive = DEFAULT_KEEP_ALIVE
      --.last_will = null
      --.max_inflight = DEFAULT_MAX_INFLIGHT:
    if max_inflight < 1: throw "INVALID_ARGUMENT"
