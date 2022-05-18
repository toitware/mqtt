// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .packets
import reader

/**
The backing transport for the MQTT client.
*/
interface Transport implements reader.Reader:
  /**
  Write to the transport.

  Returns the number of bytes written.
  */
  write bytes/ByteArray -> int

  /**
  Receives bytes from the peer.
  */
  read -> ByteArray?

  /**
  Closes the transport.

  If another task is sending or receiving, that operation must throw.
  Any future $write or $read calls must throw.

  The close operation itself must not throw.
  // TODO(florian): should we deal with disconnections that can throw?
  */
  close -> none

  /**
  Whether this transport supports reconnecting.
  */
  supports_reconnect -> bool

  /**
  Reconnects the transport.
  */
  reconnect -> none
