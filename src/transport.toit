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
  receive --timeout/Duration?=null -> Packet?

/**
A transport that can reconnect in case of failures.
*/
interface ReconnectingTransport extends Transport:
  /**
  Reconnects the transport.
  */
  reconnect -> none
