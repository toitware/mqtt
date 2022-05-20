// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor
import log
import reader
import writer

import .client_options
import .last_will
import .packets
import .tcp  // For toitdoc.
import .topic_filter
import .transport

CLIENT_CLOSED_EXCEPTION ::= "CLIENT_CLOSED"

/**
A barrier that allows multiple tasks to synchronize.
Also has a value that can be used to communicate.

Similar to $monitor.Latch but explicitly allows multiple tasks to $get.
*/
monitor Barrier_:
  has_value_ := false
  value_ := null

  /**
  Receives the value.

  Blocks until the value is available.
  */
  get:
    await: has_value_
    return value_

  /**
  Sets the $value of the barrier.

  Calling this method unblocks any task that is blocked in the $get method of
    the same instance, sending the $value to it.
  Future calls to $get return immediately and use this $value.
  Must be called at most once.
  */
  set value:
    value_ = value
    has_value_ = true

  /**
  Whether the barrier has a value.
  */
  has_value -> bool:
    return has_value_

class ActivityMonitoringTransport_ implements Transport:
  wrapped_transport_ / Transport
  is_writing /bool := false
  writing_since /int? := null
  last_sent_us /int? := null

  constructor .wrapped_transport_:

  write bytes/ByteArray -> int:
    try:
      is_writing = true
      writing_since = Time.monotonic_us
      result := wrapped_transport_.write bytes
      last_sent_us = Time.monotonic_us
      return result
    finally:
      is_writing = false

  read -> ByteArray?:
    return wrapped_transport_.read

  close -> none:
    wrapped_transport_.close

  supports_reconnect -> bool:
    return wrapped_transport_.supports_reconnect

  reconnect -> none:
    wrapped_transport_.reconnect


class ActivityChecker_:
  client_ /Client
  keep_alive_ /Duration
  current_connection_ /Connection_? := null

  constructor .client_ --keep_alive/Duration:
    keep_alive_ = keep_alive

  reset:
    // Currently do nearly nothing.
    // We will want to reset our data.
    current_connection_ = client_.connection_

  /**
  Checks for activity.

  Returns a duration for when it wants to be called again.
  Returns null if the connection is not alive anymore.
  */
  check -> Duration?:
    client_.check_connected_
    if current_connection_ != client_.connection_:
      reset

    // There should have been a 'connect' message.
    assert: transport_.is_sending or transport_.last_sent_us

    if not transport_.last_sent_us:
      assert: transport_.is_sending
      return keep_alive_

    // TODO(florian): we should be more clever here:
    // We should monitor when the transport starts writing, and when it gets a chunk through.
    // Also, we should monitor that we actually get something from the server.
    remaining_keep_alive_us := keep_alive_.in_us - (Time.monotonic_us - transport_.last_sent_us)
    if remaining_keep_alive_us > 0:
      remaining_keep_alive := Duration --us=remaining_keep_alive_us
      return remaining_keep_alive
    else if not client_.is_sending:
      // TODO(florian): we need to keep track of whether we have sent a ping.
      client_.send_ PingReqPacket --packet_id=null
      return keep_alive_ / 2
    else:
      // TODO(florian): we are currently sending.
      // We should detect timeouts on the sending.
      client_.request_ping_after_current_packet
      return keep_alive_

  transport_ -> ActivityMonitoringTransport_:
    return client_.transport_

  run:
    while not client_.is_closed and not client_.is_closing:
      duration := check
      sleep duration

class Connection_:
  /**
  The connection is considered alive.
  This could be because we haven't tried to establish a connection, but it
    could also be that we are happily running.
  */
  static STATE_ALIVE_ ::= 0
  /**
  The connection is in the process of disconnecting.
  Once connected, if there is an error during receiving or sending, the connection will
    switch to this state and call $Transport.close. This will cause the
    other side (receive or send) to shut down as well (if there is any).
  Once the handler has finished cleaning up, the state switches to $STATE_CLOSED_.
  */
  static STATE_CLOSING_ ::= 1
  /**
  The connection is shut down.
  */
  static STATE_CLOSED_ ::= 2

  state_ / int := STATE_ALIVE_

  should_send_ping_ /bool := false

  transport_ / Transport

  closing_reason_ /any := null

  writing_ /monitor.Mutex ::= monitor.Mutex

  constructor .transport_:

  is_alive -> bool: return state_ == STATE_ALIVE_
  is_closing -> bool: return state_ == STATE_CLOSING_
  is_closed -> bool: return state_ == STATE_CLOSED_

  /**
  Connects to the server and receives incoming packets.

  Only returns when the connection is closed.
  Returns null if the connection is cleanly closed.
  Throws otherwise.
  */
  handle [block]:
    try:
      catch --unwind=(: not is_closing and not is_closed):
        reader := reader.BufferedReader transport_
        while not is_closing:
          packet := Packet.deserialize reader
          block.call packet
    finally: | is_exception exception |
      close --reason=(is_exception ? exception : null)
      state_ = STATE_CLOSED_

  close --reason=null:
    if is_closing or is_closed: return
    assert: closing_reason_ == null
    closing_reason_ = reason
    // By setting the state to closing we quell any error messages from disconnecting the transport.
    state_ = STATE_CLOSING_
    transport_.close

  request_ping_after_current_message:
    assert: transport_
    should_send_ping_ = true

  send packet/Packet:
    // The client already serializes most sends. However, some messages are written without
    // taking the client's lock. For example, 'ack' messages, the 'disconnect' message, or pings
    // are directly written to the connection.
    writing_.do:
      try:
        exception := catch --unwind=(: not is_closing and not is_closed):
          writer := writer.Writer transport_
          writer.write packet.serialize
          // Let pings jump the queue.
          if should_send_ping_:
            should_send_ping_ = false
            transport_.write (PingReqPacket).serialize
        if exception:
          assert: is_closing or is_closed
          throw CLIENT_CLOSED_EXCEPTION
      finally: | is_exception exception |
        if is_exception:
          close --reason=exception

/**
A strategy to handle reconnects.

Keeps track of reconnection attempts.
*/
class ReconnectionStrategy_:


/**
An MQTT client.

The client is responsible for maintaining the connection to the server.
If necessary it reconnects to the server.

When the connection to the broker is established with the clean-session bit, the client
  resubscribes all its subscriptions. However, due to the bit, there might be some
  messages that are lost.
*/
class Client:
  /** The client has been created. Handle has not been called yet. */
  static STATE_CREATED_ ::= 0
  /** The client is connecting. */
  static STATE_CONNECTING1_ ::= 1
  static STATE_CONNECTING2_ ::= 2
  /** The client is connected. */
  static STATE_CONNECTED_ ::= 3
  /** The client is disconnected. */
  static STATE_DISCONNECTED_ ::= 4
  /**
  The client is disconnected and in the process of shutting down.
  This only happens once the current message has been handled. That is, once
    the $handle_ method's block has returned.
  */
  static STATE_CLOSING_ ::= 4
  /** The client is closed. */
  static STATE_CLOSED_ ::= 5

  state_ /int := STATE_CREATED_

  options_ /ClientOptions
  transport_ /ActivityMonitoringTransport_
  logger_ /log.Logger?

  connection_ /Connection_? := null
  connected_ /Barrier_ := Barrier_
  reconnect_done_ /monitor.Latch? := null
  reconnection_strategy_ /ReconnectionStrategy_ := ReconnectionStrategy_

  subscriptions_ /Map := {:}

  next_packet_id_/int? := 1

  pending_ / Map ::= {:}  // int -> Packet

  activity_task_ /Task_? := null

  closed_ /Barrier_ ::= Barrier_

  /**
  Serializes the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex
  is_sending_ /bool := false
  should_send_ping_ /bool := false

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $start to initiate the connection.
  */
  constructor --options/ClientOptions --transport/Transport --logger/log.Logger?:
    options_ = options
    transport_ = ActivityMonitoringTransport_(transport)
    logger_ = logger


  start --on_packet/Lambda -> none:
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_
    connect_
    handle_ --on_packet=on_packet

  handle_ --on_packet/Lambda -> none:
    try:
      activity_task_ = task --background::
        try:
          checker := ActivityChecker_ this --keep_alive=options_.keep_alive
          checker.run
        finally:
          activity_task_ = null

      with_reconnect_ --no-wait_for_connected:
        connection_.handle: | packet/Packet |
          if packet is PublishPacket:
            on_packet.call packet
          else if packet is ConnAckPacket:
            handle_connack_ (packet as ConnAckPacket)
          else if packet is PacketIDAck:
            ack := packet as PacketIDAck
            id := ack.packet_id
            // TODO(florian): implement persistence layer.
            pending_.remove id
                --if_absent=: logger_.info "unmatched packet id: $id"
    finally:
      tear_down_
      connection_ = null

  reconnect_ --reason --old_connection/Connection_ -> none:
    assert: not connection_.is_alive
    // TODO(florian): implement exponential back-off or something similar.
    if connection_ != old_connection:
      exception := reconnect_done_.get
      if exception: throw exception

    if not connected_.has_value:
      // Make sure anyone listening on the barrier makes progress.
      connected_.set reason
    connected_ = Barrier_
    connection_ = null
    should_send_ping_ = false
    reconnect_done_ = monitor.Latch
    try:
      transport_.reconnect
    finally: | is_exception exception |
      reconnect_done_.set (is_exception ? exception : null)
    connect_

  with_reconnect_ [block] --wait_for_connected/bool:
    while true:
      current_connection := connection_
      // If the connection is still alive, or if the transport doesn't support reconnection
      // anyway, don't even try to reconnect.
      exception := catch --unwind=(: connection_.is_alive or not transport_.supports_reconnect):
        if wait_for_connected: check_connected_
        current_connection = connection_
        block.call
        return
      assert: exception != null
      if is_closing or is_closed: return
      reconnect_ --reason=exception --old_connection=current_connection

  connect_:
    connection_ = Connection_ transport_
    state_ = STATE_CONNECTING1_
    packet := ConnectPacket options_.client_id
        --username=options_.username
        --password=options_.password
        --keep_alive=options_.keep_alive
        --last_will=options_.last_will
    reconnection_strategy_.inform_connect_send
    connection_.send packet
    reconnection_strategy_.inform_connect_sent
    state_ = STATE_CONNECTING2_

  handle_connack_ packet/ConnAckPacket:
    if state_ != STATE_CONNECTING2_:
      if logger_: logger_.info "Received spurious CONNACK"
      return

    if packet.return_code != 0:
      state_ = STATE_CLOSED_
      reconnection_strategy_.inform_refused
      connected_.set "connection refused: $packet.return_code"
      return

    reconnection_strategy_.inform_connected
    state_ = STATE_CONNECTED_
    connected_.set null

    // Before publishing messages, we need to subscribe to the topics we were subscribed to.
    // TODO(florian): also send/clear pending acks.
    // TODO(florian): move the sub call to the front of the queue.
    if not subscriptions_.is_empty:
      topic_list := []
      subscriptions_.do: | topic/string max_qos/int |
        topic_list.add (TopicFilter topic --max_qos=max_qos)
      subscribe_all topic_list
      packet_id := next_packet_id_++
      subscribe_packet := SubscribePacket topic_list --packet_id=packet_id
      connection_.send packet
      if packet_id: pending_[packet_id] = packet
      send_ packet --packet_id=packet_id

  /**
  Waits for the connection to be connected and then calls the given $block.

  If the connection could not connect throws.
  If the connection is closed throws.
  */
  when_connected [block]:
    check_connected_
    block.call

  check_connected_:
    exception := connected_.get
    if exception: throw exception
    // Check that we are still connected and haven't been closed in the meantime.
    if not connection_.is_alive: throw CLIENT_CLOSED_EXCEPTION

  /**
  Tears down the client.
  */
  tear_down_:
    critical_do:
      state_ = STATE_CLOSED_
      if activity_task_:
        activity_task_.cancel
        activity_task_ = null
      closed_.set true
      if not connected_.has_value:
        connected_.set CLIENT_CLOSED_EXCEPTION

  /**
  Closes the client.

  Unless the client is already closed, executes an orderly disconnect.
  */
  close:
    if is_closing or is_closed: return

    state_ = STATE_CLOSING_

    // Note that disconnect packets don't need a packet id (which is important as
    // the packet_id counter is used as marker that the client is closed).
    if connection_.is_alive:
      catch --trace: connection_.send DisconnectPacket

    // The connection disconnect will stop the $Connection_.handle. This in turn will invoke
    // the `tear_down` in $start method.
    connection_.close

    closed_.get

  /** Whether the client is closed. */
  is_closed -> bool:
    return state_ == STATE_CLOSED_

  /**
  Whether the client is closing.

  A graceful closing requires the client to send a packet to the broker. This means that
    a client can be in closing state for a while.
  */
  is_closing -> bool:
    return state_ == STATE_CLOSING_

  /**
  Publishes an MQTT message on $topic.

  The $qos parameter must be either:
  - 0: at most once, aka "fire and forget". In this configuration the message is sent, but the delivery
        is not guaranteed.
  - 1: at least once. The MQTT client ensures that the message is received by the MQTT broker.

  QOS = 2 (exactly once) is not implemented by this client.

  The $retain parameter lets the MQTT broker know whether it should retain this message. A new (later)
    subscription to this $topic would receive the retained message, instead of needing to wait for
    a new message on that topic.

  Not all MQTT brokers support $retain.
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false -> none:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet_id := qos > 0 ? next_packet_id_++ : null

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet_id=packet_id

    send_ packet --packet_id=(qos > 0 ? packet_id : null)

  /**
  Subscribes to the given $filter with a max qos of $max_qos.
  */
  subscribe filter/string --max_qos/int=1 -> none:
    subscribe_all [ TopicFilter filter --max_qos=max_qos ]

  /**
  Subscribes to the given list $topic_filters of type $TopicFilter.
  */
  subscribe_all topic_filters/List -> none:
    if topic_filters.is_empty: throw "INVALID_ARGUMENT"
    if is_closed: throw CLIENT_CLOSED_EXCEPTION

    topic_filters.do: | topic_filter/TopicFilter |
      subscriptions_[topic_filter.filter] = topic_filter.max_qos

    packet_id := next_packet_id_++
    packet := SubscribePacket topic_filters --packet_id=packet_id
    send_ packet --packet_id=packet_id

  /**
  Unsubscribes from a single topic $filter.
  */
  unsubscribe filter/string -> none:
    // Not implemented yet.

  request_ping_after_current_packet:
    should_send_ping_ = true

  is_sending -> bool:
    return is_sending_

  send_ packet/Packet --packet_id/int? -> none:
    sending_.do:
      if is_closing or is_closed: throw CLIENT_CLOSED_EXCEPTION
      is_sending_ = true
      try:
        if packet is ConnectPacket: throw "INVALID_PACKET"
        with_reconnect_ --wait_for_connected:
          connection_.send packet
          if packet_id: pending_[packet_id] = packet
          if should_send_ping_:
            should_send_ping_ = false
            connection_.send PingReqPacket
      finally:
        is_sending_ = false

  /**
  Acknowledges the hand-over of the packet.

  If the packet has qos=1, sends an ack packet to the broker.
  */
  ack packet/Packet:
    // Can't ack if we don't have a connection anymore.
    if is_closing or is_closed: return
    if packet is PublishPacket:
      id := (packet as PublishPacket).packet_id
      if id:
        ack := PubAckPacket id
        connection_.send ack
