// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .last-will

/**
Options to connect to an MQTT broker.
*/
class SessionOptions:
  static DEFAULT-KEEP-ALIVE ::= Duration --s=60
  static DEFAULT-MAX-PENDING ::= 20

  client-id     /string
  clean-session /bool
  username      /string?
  password      /string?
  keep-alive    /Duration
  last-will     /LastWill?
  max-pending   /int

  /**
  The $client-id identifies this session. An empty ID requires clean-session;
    otherwise use a stable, broker-unique UTF-8 identifier. Broker limits apply.

  If necessary, the $username/$password credentials can be used to authenticate.

  The $keep-alive informs the server of the maximum duration between two packets.
    The client automatically sends PINGREQ messages when necessary. If the value is
    lower, then the server detects disconnects faster, but the client needs to send
    more messages.
  If $keep-alive is set to 0, the broker does not disconnect due to inactivity, and
    the client won't send any ping requests.

  When provided, the $last-will configuration is used to send when the client
    disconnects ungracefully.

  The $max-pending parameter bounds all accepted operations, including queued
    publishes, subscriptions, and unsubscriptions. Admission blocks until an
    operation completes or the client closes. Separate byte limits are configured
    on Client. Keepalive is a whole number of seconds; zero disables pings.
  */
  constructor
      --.client-id
      --.clean-session = false
      --.username = null
      --.password = null
      --.keep-alive = DEFAULT-KEEP-ALIVE
      --.last-will = null
      --.max-pending = DEFAULT-MAX-PENDING:
    if not 1 <= max-pending <= 65_000: throw "INVALID_ARGUMENT"
