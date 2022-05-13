// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .packets

/**
The backing transport for the MQTT client.
*/
interface Transport:
  /**
  Send a packet to the peer.
  */
  send packet/Packet -> none

  /**
  Receive the next packet from the peer.

  Returns null if timeout was exceeded.
  */
  receive --timeout/Duration? -> Packet?

  /**
  Close the transport.

  If another task is sending or receiving, that operation must throw.
  Any future $send or $receive calls must throw.

  The disconnection operation itself must not throw.
  // TODO(florian): should we deal with disconnections that can throw?
  */
  disconnect -> none

  /**
  Whether this transport supports reconnecting.
  */
  supports_reconnect -> bool

  /**
  Reconnects the transport.
  */
  reconnect -> none
