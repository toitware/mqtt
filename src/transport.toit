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
  Writes to the transport.

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
  */
  close -> none

  /**
  Whether this transport supports reconnecting.
  */
  supports_reconnect -> bool

  /**
  Reconnects the transport.

  If the transport $supports_reconnect, it should try to reconnect.
  Normally, the transport is in a closed state when reconnect is called.
  */
  reconnect -> none

  /**
  Whether the transport is closed.
  If it $supports_reconnect then calling $reconnect reopens the transport.
  */
  is_closed -> bool

/**
A transport that monitors activity on a wrapped transport.

The MQTT library automatically wraps transports in actvity-monitoring
  transports. Users of the library should never need to instantiate this
  class themselves.
*/
class ActivityMonitoringTransport implements Transport:
  wrapped_transport_ / Transport

  is_writing /bool := false
  writing_since_us /int? := null
  last_write_us /int? := null

  is_reading /bool := false
  reading_since_us /int? := null
  last_read_us /int? := null

  constructor.private_ .wrapped_transport_:

  write bytes/ByteArray -> int:
    try:
      is_writing = true
      writing_since_us = Time.monotonic_us
      result := wrapped_transport_.write bytes
      last_write_us = Time.monotonic_us
      return result
    finally:
      is_writing = false

  read -> ByteArray?:
    try:
      is_reading = true
      reading_since_us = Time.monotonic_us
      result := wrapped_transport_.read
      last_read_us = Time.monotonic_us
      return result
    finally:
      is_reading = false

  close -> none:
    wrapped_transport_.close

  supports_reconnect -> bool:
    return wrapped_transport_.supports_reconnect

  reconnect -> none:
    wrapped_transport_.reconnect

  is_closed -> bool:
    return wrapped_transport_.is_closed
