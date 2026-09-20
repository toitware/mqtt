// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import .client-state
import .completion
import .errors
import .link
import .packets
import .retry
import .session-options
import .topic-qos
import .topics
import .wire

export Message Receipt Completion_

/**
A bounded MQTT 3.1.1 client with one owner for its lifetime and session.

Start creates the owner task. Observe terminal failure through wait-closed, an
  operation receipt, or receive. Transient connection failures keep accepted
  operations pending. Application code never runs on the protocol reader.
*/
class Client:
  connector_/Connector
  options_/SessionOptions
  retry_/RetryPolicy
  wire_/Wire
  state_/ClientState_
  timeout_/Duration
  max-subscriptions_/int
  subscriptions_/Map := {:}
  pending_/Map := {:}
  next-id_/int := 1
  generation_/int := 0
  started_/bool := false
  owner_/Task? := null
  reader_/Task? := null
  connection_/PacketConnection_? := null
  connected-since_/int? := null

  constructor --connector/Connector --options/SessionOptions
      --retry/RetryPolicy=RetryPolicy
      --max-packet-size/int=16_384
      --max-pending-bytes/int=65_536
      --max-incoming/int=16
      --max-incoming-bytes/int=65_536
      --max-subscriptions/int=64
      --io-timeout/Duration=(Duration --s=10):
    if options.max-pending > 65_000 or max-subscriptions < 1 or max-subscriptions > 256 or io-timeout.in-us <= 0:
      throw "INVALID_ARGUMENT"
    if options.keep-alive.in-us < 0 or options.keep-alive.in-s > 0xffff:
      throw "INVALID_ARGUMENT"
    // MQTT encodes keepalive in whole seconds.
    if options.keep-alive.in-us % 1_000_000 != 0: throw "INVALID_ARGUMENT"
    connector_ = connector
    options_ = SessionOptions --client-id=options.client-id
        --clean-session=options.clean-session
        --username=options.username
        --password=options.password
        --keep-alive=options.keep-alive
        --last-will=options.last-will
        --max-pending=options.max-pending
    retry_ = retry
    wire_ = Wire --max-packet-size=max-packet-size
    state_ = ClientState_ options.max-pending max-pending-bytes max-incoming max-incoming-bytes
    timeout_ = io-timeout
    max-subscriptions_ = max-subscriptions
    // Reject invalid CONNECT options before starting any background work.
    wire_.encode connect-packet_

  /** The current lifecycle state: created, connecting, online, reconnecting, closing, closed, or failed. */
  connection-state -> string:
    return state_.status

  /** The most recent attempt failure, retained even after recovery. */
  last-connection-error -> any:
    return state_.last-connection-error

  /** Starts the lifetime owner. Call exactly once. */
  start -> none:
    if started_: throw "ALREADY_STARTED"
    started_ = true
    owner_ = task --background:: run_

  /** Waits for an online connection, or throws if the client stops. */
  wait-connected -> none:
    check-started_
    state_.wait-online

  /** Waits for cleanup; throws the stored terminal failure, if any. */
  wait-closed -> none:
    check-started_
    state_.lifetime.wait

  /**
  Requests shutdown. Use wait-closed to wait for cleanup.

  Normal shutdown sends DISCONNECT if online, within the I/O timeout. Force
    aborts the attempt immediately. Accepted but unacknowledged operations fail
    with CLIENT_CLOSED; already completed receipts remain unchanged.
  */
  close --force/bool=false -> none:
    state_.request-close
    if not started_:
      started_ = true
      state_.finish
    else if owner_ and (force or not connected-since_):
      owner_.cancel

  /**
  Accepts a publish, blocking only for bounded admission capacity.

  The payload is copied on admission. For QoS 1 the returned receipt completes
    on PUBACK; for QoS 0 it completes after the write. Reconnection may duplicate
    QoS 1 delivery. Timing out on the receipt does not cancel that delivery.
  */
  publish topic/string payload/io.Data --qos/int=1 --retain/bool=false -> Receipt:
    check-started_
    validate-topic topic
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"
    size := topic.size + payload.byte-size + 9
    check-size_ size
    return state_.submit size:
      bytes := ByteArray.from payload
      packet := PublishPacket topic bytes --qos=qos --retain=retain --packet-id=(qos == 1 ? 1 : null)
      Operation_ packet size

  /** Subscribes to a filter; the receipt returns its granted QoS or throws. */
  subscribe filter/string --max-qos/int=1 -> Receipt:
    check-started_
    validate-filter filter
    if max-qos != 0 and max-qos != 1: throw "INVALID_ARGUMENT"
    size := filter.size + 10
    check-size_ size
    return state_.submit size:
      Operation_ (SubscribePacket [TopicQos filter --max-qos=max-qos] --packet-id=1) size

  /** Unsubscribes from a filter; the receipt completes on UNSUBACK. */
  unsubscribe filter/string -> Receipt:
    check-started_
    validate-filter filter
    size := filter.size + 9
    check-size_ size
    return state_.submit size:
      Operation_ (UnsubscribePacket [filter] --packet-id=1) size

  /**
  Receives a complete message. Drains accepted messages before reporting closure.

  Returns null after normal closure; throws the stored cause after failure.
    The caller owns delivery and may invoke handlers or publish without blocking
    the protocol reader. Failure to drain the bounded inbox fails the lifetime.
  */
  receive -> Message?:
    check-started_
    return state_.receive

  check-started_:
    if not started_: throw "NOT_STARTED"

  check-size_ size/int:
    if size > wire_.max-packet-size: throw (CapacityError "packet bytes")

  connect-packet_ -> ConnectPacket:
    return ConnectPacket options_.client-id --clean-session=options_.clean-session
        --username=options_.username
        --password=options_.password
        --keep-alive=options_.keep-alive
        --last-will=options_.last-will

  run_:
    terminal/any := null
    attempt := 0
    try:
      while not state_.stopping:
        state_.begin-attempt
        failure := catch: connect-and-run_
        if state_.stopping: break
        if not failure: failure = ConnectionError "CONNECTION_CLOSED" null
        if connected-since_ and Time.monotonic-us - connected-since_ >= retry_.stable-after.in-us:
          attempt = 0
        connected-since_ = null
        state_.record-failure failure
        delay := retry_.delay failure attempt++
        if delay == null:
          terminal = failure
          break
        sleep delay
    finally:
      // Also runs when close cancels a connecting or sleeping owner.
      critical-do:
        release-connection_
        state_.finish --failure=terminal
        pending_.clear
        subscriptions_.clear
        owner_ = null

  connect-and-run_:
    try:
      link/Link? := null
      failure := catch:
        with-timeout timeout_: link = connector_.open
      if failure: throw (ConnectionError "CONNECT_FAILED" failure)
      connection_ = PacketConnection_ link wire_ --write-timeout=timeout_
      generation_++
      response/Packet? := null
      failure = catch:
        with-timeout timeout_:
          connection_.write connect-packet_
          response = connection_.read
      if failure:
        if failure is MqttError: throw failure
        throw (ConnectionError "HANDSHAKE_FAILED" failure)
      if response is not ConnAckPacket: throw (ProtocolError "expected CONNACK")
      ack := response as ConnAckPacket
      if ack.return-code == 3: throw (ConnectionError "SERVER_UNAVAILABLE" ack.return-code)
      if ack.return-code != 0: throw (MqttError "CONNECTION_REFUSED" --cause=ack.return-code)
      if options_.clean-session and ack.session-present: throw (ProtocolError "unexpected session")
      if state_.stopping: return
      connected-since_ = Time.monotonic-us
      keepalive := KeepAlive_ options_.keep-alive connected-since_
      start-reader_ connection_ generation_
      restore-session_ ack.session-present keepalive
      state_.set-online true
      while not state_.stopping:
        if keepalive.ping-expired Time.monotonic-us:
          throw (ConnectionError "PING_TIMEOUT" null)
        if keepalive.ping-due Time.monotonic-us:
          connection_.write PingReqPacket
          keepalive.wrote Time.monotonic-us --ping
        event := state_.next --deadline=keepalive.next-deadline
        if event is Operation_:
          send-operation_ event keepalive
        else if event is PacketEvent_:
          received := event as PacketEvent_
          if received.generation != generation_: continue
          if received.failure: throw received.failure
          if not received.packet: throw (ConnectionError "CONNECTION_CLOSED" null)
          handle-packet_ received.packet keepalive
      connection_.write DisconnectPacket
    finally:
      critical-do: release-connection_

  start-reader_ connection/PacketConnection_ generation/int:
    reader_ = task --background::
      while not Task.current.is-canceled:
        packet/Packet? := null
        failure := catch: packet = connection.read
        if Task.current.is-canceled: break
        state_.received (PacketEvent_ generation packet --failure=failure)
        if failure or not packet: break

  release-connection_:
    state_.set-online false
    if reader_:
      reader_.cancel
      reader_ = null
    if connection_:
      connection_.close
      connection_ = null

  allocate-id_ -> int:
    0xffff.repeat:
      id := next-id_
      next-id_ = id == 0xffff ? 1 : id + 1
      if not pending_.contains id: return id
    throw (CapacityError "packet identifiers")

  send-operation_ operation/Operation_ keepalive/KeepAlive_:
    packet := operation.packet
    if packet is PublishPacket:
      publish := packet as PublishPacket
      if publish.qos == 0:
        // QoS 0 has no retry receipt: a failed write is an uncertain delivery.
        failure := catch: connection_.write packet
        state_.complete operation --failure=failure
        if failure: throw failure
        keepalive.wrote Time.monotonic-us
        return
      packet = publish.with --packet-id=allocate-id_
      pending_[(packet as PublishPacket).packet-id] = operation
    else if packet is SubscribePacket:
      subscribe := packet as SubscribePacket
      filter := subscribe.topics.first.topic
      if not subscriptions_.contains filter and subscriptions_.size >= max-subscriptions_:
        state_.complete operation --failure=(CapacityError "subscriptions")
        return
      subscriptions_[filter] = subscribe.topics.first.max-qos
      packet = SubscribePacket subscribe.topics --packet-id=allocate-id_
      pending_[(packet as SubscribePacket).packet-id] = operation
    else:
      unsubscribe := packet as UnsubscribePacket
      subscriptions_.remove unsubscribe.topics.first
      packet = UnsubscribePacket unsubscribe.topics --packet-id=allocate-id_
      pending_[(packet as UnsubscribePacket).packet-id] = operation
    operation.packet = packet
    connection_.write packet
    keepalive.wrote Time.monotonic-us

  restore-session_ had-session/bool keepalive/KeepAlive_:
    changing := {}
    pending_.do --values: | operation/Operation_ |
      packet := operation.packet
      if packet is SubscribePacket: changing.add (packet as SubscribePacket).topics.first.topic
      if packet is UnsubscribePacket: changing.add (packet as UnsubscribePacket).topics.first
      if packet is PublishPacket:
        packet = (packet as PublishPacket).with --duplicate
      connection_.write packet
      keepalive.wrote Time.monotonic-us
    if had-session: return
    subscriptions_.do: | filter/string qos/int |
      if changing.contains filter: continue.do
      packet := SubscribePacket [TopicQos filter --max-qos=qos] --packet-id=1
      send-operation_ (Operation_ packet 0 --internal) keepalive

  handle-packet_ packet/Packet keepalive/KeepAlive_:
    if packet is PingRespPacket:
      keepalive.received-pong
      return
    if packet is PublishPacket:
      publish := packet as PublishPacket
      state_.deliver publish
      if publish.qos == 1:
        connection_.write (PubAckPacket --packet-id=publish.packet-id)
        keepalive.wrote Time.monotonic-us
      return
    if packet is not AckPacket: throw (ProtocolError "unexpected packet")
    id := (packet as AckPacket).packet-id
    operation/Operation_? := pending_.get id
    if not operation: return
    sent := operation.packet
    if sent is PublishPacket:
      if packet is not PubAckPacket: throw (ProtocolError "expected PUBACK")
    else if sent is SubscribePacket:
      if packet is not SubAckPacket: throw (ProtocolError "expected SUBACK")
      ack := packet as SubAckPacket
      if ack.qos.size != 1: throw (ProtocolError "SUBACK count")
      filter := (sent as SubscribePacket).topics.first.topic
      if ack.qos.first == 0x80:
        failure := MqttError "SUBSCRIPTION_REJECTED" --cause=filter
        pending_.remove id
        subscriptions_.remove filter
        state_.complete operation --failure=failure
        if operation.internal: throw failure
        return
      if ack.qos.first > (sent as SubscribePacket).topics.first.max-qos:
        throw (ProtocolError "SUBACK exceeds requested QoS")
      subscriptions_[filter] = ack.qos.first
    else:
      if packet is not UnsubAckPacket: throw (ProtocolError "expected UNSUBACK")
      subscriptions_.remove (sent as UnsubscribePacket).topics.first
    pending_.remove id
    state_.complete operation --value=(packet is SubAckPacket ? (packet as SubAckPacket).qos.first : null)
