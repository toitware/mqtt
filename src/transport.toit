// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import .packets
import .utils_

/**
The backing transport for the MQTT client.
*/
interface Transport:
  /**
  Writes to the transport.

  Returns the number of bytes written.
  */
  write data/ByteArray -> int

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
  supports-reconnect -> bool

  /**
  Reconnects the transport.

  If the transport $supports-reconnect, it should try to reconnect.
  Normally, the transport is in a closed state when reconnect is called.
  */
  reconnect -> none

  /**
  Disconnects the transport.

  This can be called before doing a $reconnect. It will close the network, if
    that's supported.
  Disconnecting is not the same as closing. A transport can be disconnected
    and still be open.
  */
  disconnect -> none

  /**
  Whether the transport is closed.
  If it $supports-reconnect then calling $reconnect reopens the transport.
  */
  is-closed -> bool


/**
A transport that monitors activity on a wrapped transport.

The MQTT library automatically wraps transports in actvity-monitoring
  transports. Users of the library should never need to instantiate this
  class themselves.
*/
class ActivityMonitoringTransport_ implements Transport:
  wrapped-transport_ / Transport

  is-writing /bool := false
  writing-since-us /int? := null
  last-write-us /int? := null

  is-reading /bool := false
  reading-since-us /int? := null
  last-read-us /int? := null

  constructor.private_ .wrapped-transport_:

  write bytes/ByteArray -> int:
    try:
      is-writing = true
      writing-since-us = Time.monotonic-us
      result := wrapped-transport_.write bytes
      last-write-us = Time.monotonic-us
      return result
    finally:
      is-writing = false

  read -> ByteArray?:
    try:
      is-reading = true
      reading-since-us = Time.monotonic-us
      result := wrapped-transport_.read
      last-read-us = Time.monotonic-us
      return result
    finally:
      is-reading = false

  close -> none:
    wrapped-transport_.close

  supports-reconnect -> bool:
    return wrapped-transport_.supports-reconnect

  reconnect -> none:
    wrapped-transport_.reconnect

  disconnect -> none:
    wrapped-transport_.disconnect

  is-closed -> bool:
    return wrapped-transport_.is-closed

class TransportWriter_ extends io.Writer:
  transport_ /Transport

  constructor .transport_:

  try-write_ data/io.Data from/int to/int -> int:
    return transport_.write (io-data-to-byte-array_ data from to)

class TransportReader_ extends io.Reader:
  transport_ /Transport

  constructor .transport_:

  consume_ -> ByteArray?:
    return transport_.read
