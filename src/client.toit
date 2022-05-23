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

/**
A connection to the broker.

Primarily ensures that exceptions are handled correctly.
The first side (reading or writing) that detects an issue with the transport disconnects the transport.
  It throws the original exception. The other side simply throws a "CLIENT_CLOSED_EXCEPTION".
*/
class Connection_:
  /**
  The connection is considered alive.
  This could be because we haven't tried to establish a connection, but it
    could also be that we are happily running.
  */
  static STATE_ALIVE_ ::= 0
  /**
  The connection is closed or in the process of closing.
  Once connected, if there is an error during receiving or sending, the connection will
    switch to this state and call $Transport.close. This will cause the
    other side (receive or send) to shut down as well (if there is any).
  */
  static STATE_CLOSED_ ::= 1

  state_ / int := STATE_ALIVE_

  transport_ / Transport
  reader_ /reader.BufferedReader
  writer_ /writer.Writer
  writing_ /monitor.Mutex ::= monitor.Mutex

  closing_reason_ /any := null

  constructor .transport_:
    reader_ = reader.BufferedReader transport_
    writer_ = writer.Writer transport_

  is_alive -> bool: return state_ == STATE_ALIVE_
  is_closed -> bool: return state_ == STATE_CLOSED_

  /**
  Receives an incoming packet.
  */
  read -> Packet?:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    try:
      catch --unwind=(: not is_closed):
        return Packet.deserialize reader_

      if closing_reason_: throw closing_reason_
      return null
    finally: | is_exception exception |
      if is_exception:
        close --reason=exception
        state_ = STATE_CLOSED_

  close --reason=null:
    if is_closed: return
    assert: closing_reason_ == null
    closing_reason_ = reason
    // By setting the state to closied we quell any error messages from disconnecting the transport.
    state_ = STATE_CLOSED_
    transport_.close

  write packet/Packet:
    // The client already serializes most sends. However, some messages are written without
    // taking the client's lock. For example, 'ack' messages, the 'disconnect' message, or pings
    // are directly written to the connection (jumping the queue).
    writing_.do:
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      try:
        exception := catch --unwind=(: not is_closed):
          writer_.write packet.serialize
        if exception:
          assert: is_closed
          throw CLIENT_CLOSED_EXCEPTION
      finally: | is_exception exception |
        if is_exception:
          close --reason=exception

/**
Establishes and maintains a connection to the broker.
*/
interface ConnectionManager:
  // Must return the result of `receive_connect_ack`.
  reconnect transport/ActivityMonitoringTransport [--send_connect] [--receive_connect_ack] -> any
  should_try_reconnect transport/ActivityMonitoringTransport -> bool
  close -> none

class DefaultConnectionManager implements ConnectionManager:
  static SEND_CONNECT_TIME_OUT_SECONDS_ /int ::= 5
  static RECEIVE_CONNECT_TIME_OUT_SECONDS_ /int ::= 5
  static ATTEMPT_DELAYS_MS_ /List ::= [1_000, 5_000, 15_000]

  is_closed /bool := false

  reconnect transport/ActivityMonitoringTransport [--send_connect] [--receive_connect_ack] -> any:
    failed_counter := 0
    while not is_closed:
      did_connect := false

      should_abandon := :
        // If we did connect, we managed to send and receive packets.
        // As such, we consider the network as "working" and will retry.
        // If we didn't manage to connect, we will only retry if we haven't exhausted
        //   the retry-attempts.
        is_closed or (not did_connect and failed_counter >= ATTEMPT_DELAYS_MS_.size)

      catch --unwind=should_abandon:
        with_timeout --ms=(SEND_CONNECT_TIME_OUT_SECONDS_ * 1000):
          send_connect.call
        ack := null
        with_timeout --ms=(RECEIVE_CONNECT_TIME_OUT_SECONDS_ * 1000):
          ack = receive_connect_ack.call

        did_connect = true

        // If the client is not authorized to connect, then the ack packet will contain an
        // error code. The caller of this method will then call the $close method, so that it
        // won't try again (which would be pointless).
        // Also, if there is an exception independent of the connection, then the client also
        // closes the connection and returns.
        return ack

      if is_closed: return null

      failed_counter++
      assert: failed_counter <= ATTEMPT_DELAYS_MS_.size
      // TODO(florian): should we split the sleep into smaller portions so that we can check if
      // the is_closed flag is set?
      sleep --ms=ATTEMPT_DELAYS_MS_[failed_counter - 1]

    return null

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return transport.supports_reconnect

  close -> none:
    is_closed = true

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
    assert: transport_.is_sending or transport_.last_write_us

    if not transport_.last_write_us:
      assert: transport_.is_sending
      return keep_alive_

    // TODO(florian): we should be more clever here:
    // We should monitor when the transport starts writing, and when it gets a chunk through.
    // Also, we should monitor that we actually get something from the server.
    remaining_keep_alive_us := keep_alive_.in_us - (Time.monotonic_us - transport_.last_write_us)
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

  transport_ -> ActivityMonitoringTransport:
    return client_.transport_

  run:
    while not client_.is_closed and not client_.is_closing:
      catch:
        duration := check
        sleep duration


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
  transport_ /ActivityMonitoringTransport
  logger_ /log.Logger?

  connection_ /Connection_? := null
  connected_ /Barrier_ := Barrier_
  reconnect_done_ /monitor.Latch? := null

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

  connection_manager_ /ConnectionManager
  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $start to initiate the connection.
  */
  constructor
      --options/ClientOptions
      --transport/Transport
      --logger/log.Logger?
      --connection_manager /ConnectionManager = DefaultConnectionManager:
    options_ = options
    transport_ = ActivityMonitoringTransport.private_(transport)
    logger_ = logger
    connection_manager_ = connection_manager

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

      with_reconnect_:
        while packet := connection_.read:
          if packet is PublishPacket:
            try:
              on_packet.call packet
            finally: | is_exception _ |
              // Exceptions in the callback bring down the Mqtt client.
              // This is harsh, but we don't want to lose messages.
              if is_exception: connection_manager_.close
          else if packet is ConnAckPacket:
            if logger_: logger_.info "spurious conn-ack packet"
          else if packet is PacketIDAck:
            ack := packet as PacketIDAck
            id := ack.packet_id
            // TODO(florian): implement persistence layer.
            pending_.remove id
                --if_absent=: logger_.info "unmatched packet id: $id"
          else:
            if logger_: logger_.info "unexpected packet of type $packet.type"
    finally:
      tear_down_
      connection_ = null

  with_reconnect_ [block]:
    while true:
      current_connection := connection_
      // If the connection is still alive, or if the manager doesn't want us to reconnect, let the
      // exception go through.
      exception := catch --unwind=(: connection_.is_alive or not connection_manager_.should_try_reconnect transport_):
        if wait_for_connected: check_connected_
        current_connection = connection_
        block.call
        return
      assert: exception != null
      if is_closing or is_closed: return
      reconnect_ --reason=exception --old_connection=current_connection

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
    connection_.send packet
    state_ = STATE_CONNECTING2_

  handle_connack_ packet/ConnAckPacket:
    if state_ != STATE_CONNECTING2_:
      if logger_: logger_.info "Received spurious CONNACK"
      return

    if packet.return_code != 0:
      state_ = STATE_CLOSED_
      connected_.set "connection refused: $packet.return_code"
      return

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

  /** Unsubscribes from a single topic $filter. */
  unsubscribe filter/string -> none:
    unsubscribe_all [filter]

  /** Unsubscribes from the list of topic $filters. */
  unsubscribe_all filters/List -> none:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    packet_id := next_packet_id_++
    packet := UnsubscribePacket filters --packet_id=packet_id
    wait_for_ack_ packet_id: | latch/monitor.Latch |
      send_ packet
      ack := latch.get
      if not ack: throw CLIENT_CLOSED_EXCEPTION

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
