// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/** A failure with its original cause and the operation that detected it. */
class MqttError:
  kind/string
  cause/any
  constructor .kind --.cause=null:
  stringify -> string:
    return cause ? "$kind: $cause" : kind

/** A malformed or unsupported MQTT packet. */
class ProtocolError extends MqttError:
  constructor cause:
    super "PROTOCOL_ERROR" --cause=cause

/** A failure of one connection attempt. */
class ConnectionError extends MqttError:
  constructor origin/string cause:
    super origin --cause=cause

/** A configured resource limit was reached. */
class CapacityError extends MqttError:
  constructor resource/string:
    super "CAPACITY_EXCEEDED" --cause=resource
