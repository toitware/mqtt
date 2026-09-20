// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .completion
import .errors
import .packets

/** A complete incoming message, owned by its receiver. */
class Message:
  topic/string
  payload/ByteArray
  qos/int
  retain/bool
  duplicate/bool
  constructor packet/PublishPacket:
    topic = packet.topic
    payload = packet.payload
    qos = packet.qos
    retain = packet.retain
    duplicate = packet.duplicate

class Operation_:
  packet/Packet := ?
  receipt/Completion_ := Completion_
  bytes/int
  internal/bool
  constructor .packet .bytes --.internal=false:

class PacketEvent_:
  generation/int
  packet/Packet?
  failure/any
  bytes/int
  constructor .generation .packet --.failure=null:
    bytes = packet ? packet.payload.size + packet.variable-header.size + 5 : 0

/**
The admission and notification boundary. No network or user callback runs here.

Accepted operations remain charged until completion, including while disconnected.
  All blocking predicates include terminal/closing state.
*/
monitor ClientState_:
  max-operations_/int
  max-bytes_/int
  max-incoming_/int
  max-incoming-bytes_/int
  events_/Deque := Deque
  event-count_/int := 0
  event-bytes_/int := 0
  active_/List := []
  bytes_/int := 0
  incoming_/Deque := Deque
  incoming-bytes_/int := 0
  stopping/bool := false
  terminal_/bool := false
  failure_/any := null
  online_/bool := false
  status/string := "created"
  last-connection-error/any := null
  lifetime/Completion_ := Completion_

  constructor .max-operations_ .max-bytes_ .max-incoming_ .max-incoming-bytes_:
    if max-operations_ < 1 or max-bytes_ < 1 or max-incoming_ < 1 or max-incoming-bytes_ < 1:
      throw "INVALID_ARGUMENT"

  submit bytes/int [create] -> Receipt:
    if bytes > max-bytes_: throw (CapacityError "outgoing bytes")
    await: stopping or (active_.size < max-operations_ and bytes_ + bytes <= max-bytes_)
    check-open_
    operation/Operation_ := create.call
    active_.add operation
    bytes_ += bytes
    events_.add operation
    return operation.receipt

  received event/PacketEvent_:
    if event.bytes > max-incoming-bytes_:
      event = PacketEvent_ event.generation null --failure=(CapacityError "incoming packet bytes")
    await: stopping or (event-count_ < max-incoming_ and event-bytes_ + event.bytes <= max-incoming-bytes_)
    if stopping: return
    events_.add event
    event-count_++
    event-bytes_ += event.bytes

  next --deadline/int?=null -> any:
    if deadline == null:
      await: stopping or not events_.is-empty
    else:
      try-await --deadline=deadline: stopping or not events_.is-empty
    if stopping or events_.is-empty: return null
    event := events_.remove-first
    if event is PacketEvent_:
      event-count_--
      event-bytes_ -= (event as PacketEvent_).bytes
    return event

  complete operation/Operation_ --value=null --failure=null:
    if not operation.internal and active_.contains operation:
      active_.remove operation
      bytes_ -= operation.bytes
    operation.receipt.complete value --failure=failure

  deliver packet/PublishPacket:
    size := packet.topic.size + packet.payload.size
    if incoming_.size >= max-incoming_ or incoming-bytes_ + size > max-incoming-bytes_:
      throw (CapacityError "incoming messages")
    incoming_.add (Message packet)
    incoming-bytes_ += size

  receive -> Message?:
    await: terminal_ or not incoming_.is-empty
    if not incoming_.is-empty:
      message/Message := incoming_.remove-first
      incoming-bytes_ -= message.topic.size + message.payload.size
      return message
    if failure_: throw failure_
    return null

  begin-attempt:
    if not stopping: status = "connecting"

  record-failure failure/any:
    last-connection-error = failure
    if not stopping: status = "reconnecting"

  set-online value/bool:
    online_ = value
    if value: status = "online"

  wait-online:
    await: stopping or online_
    check-open_

  request-close:
    if not terminal_: status = "closing"
    stopping = true
    online_ = false

  finish --failure=null:
    if terminal_: return
    stopping = true
    terminal_ = true
    online_ = false
    failure_ = failure
    status = failure ? "failed" : "closed"
    operation-failure := failure or MqttError "CLIENT_CLOSED"
    active_.do: it.receipt.complete --failure=operation-failure
    active_.clear
    events_.clear
    bytes_ = 0
    event-count_ = 0
    lifetime.complete --failure=failure

  check-open_:
    if stopping: throw (failure_ or MqttError "CLIENT_CLOSED")
