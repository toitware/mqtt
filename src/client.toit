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
A class that ensures that the connection to the broker is kept alive.

When necessary sends ping packets to the broker.
*/
class ActivityChecker_:
  connection_ /Connection_
  keep_alive_ /Duration
  current_connection_ /Connection_? := null

  constructor .connection_ --keep_alive/Duration:
    keep_alive_ = keep_alive

  /**
  Checks for activity.

  Returns a duration for when it wants to be called again.
  */
  check -> Duration:
    // TODO(florian): we should be more clever here:
    // We should monitor when the transport starts writing, and when it gets a chunk through.
    // Also, we should monitor that we actually get something from the server.
    last_write_us := connection_.transport_.last_write_us
    remaining_keep_alive_us := keep_alive_.in_us - (Time.monotonic_us - last_write_us)
    if remaining_keep_alive_us > 0:
      remaining_keep_alive := Duration --us=remaining_keep_alive_us
      return remaining_keep_alive
    else if not connection_.is_writing:
      // TODO(florian): we need to keep track of whether we have sent a ping.
      connection_.write PingReqPacket
      return keep_alive_ / 2
    else:
      // TODO(florian): we are currently sending.
      // We should detect timeouts on the sending.
      connection_.request_ping_after_current_packet
      return keep_alive_

  run:
    while not connection_.is_closed:
      catch:
        duration := check
        sleep duration


/**
A connection to the broker.

Primarily ensures that exceptions are handled correctly.
The first side (reading or writing) that detects an issue with the transport disconnects the transport.
  It throws the original exception. The other side simply throws a "CLIENT_CLOSED_EXCEPTION".

Also sends ping requests when necessary.
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

  transport_ / ActivityMonitoringTransport
  reader_ /reader.BufferedReader
  writer_ /writer.Writer
  writing_ /monitor.Mutex ::= monitor.Mutex
  is_writing_ /bool := false
  should_write_ping_ /bool := false

  closing_reason_ /any := null

  keep_alive_duration_ /Duration
  activity_task_ /Task_? := null

  constructor .transport_ --keep_alive:
    reader_ = reader.BufferedReader transport_
    writer_ = writer.Writer transport_
    keep_alive_duration_ = keep_alive


  is_alive -> bool: return state_ == STATE_ALIVE_
  is_closed -> bool: return state_ == STATE_CLOSED_

  /**
  Starts a task to keep this connection to the broker alive.
  Sends ping requests when necessary.
  */
  keep_alive --background/bool:
    assert: background
    activity_task_ = task --background::
      try:
        checker := ActivityChecker_ this --keep_alive=keep_alive_duration_
        checker.run
      finally:
        activity_task_ = null

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
    critical_do:
      closing_reason_ = reason
      // By setting the state to closied we quell any error messages from disconnecting the transport.
      state_ = STATE_CLOSED_
      catch: transport_.close
      if activity_task_:
        activity_task_.cancel
        activity_task_ = null

  is_writing -> bool:
    return is_writing_

  request_ping_after_current_packet:
    should_write_ping_ = true

  write packet/Packet:
    // The client already serializes most sends. However, some messages are written without
    // taking the client's lock. For example, 'ack' messages, the 'disconnect' message, or pings
    // are directly written to the connection (jumping the queue).
    writing_.do:
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      try:
        is_writing_ = true
        exception := catch --unwind=(: not is_closed):
          writer_.write packet.serialize
          if should_write_ping_:
            should_write_ping_ = false
            writer_.write (PingReqPacket).serialize
        if exception:
          assert: is_closed
          throw CLIENT_CLOSED_EXCEPTION
      finally: | is_exception exception |
        is_writing_ = false
        if is_exception:
          close --reason=exception

/**
A strategy to connect to the broker.

This class deals with (temporary) disconnections and timeouts.
It keeps track of whether trying to connect again makes sense, and when it
  should try again.

It is also called for the first time the client connects to the broker.
*/
interface ReconnectionStrategy:
  // Must return the result of `receive_connect_ack`.
  connect transport/ActivityMonitoringTransport [--send_connect] [--receive_connect_ack] -> any
  should_try_reconnect transport/ActivityMonitoringTransport -> bool
  close -> none

class DefaultReconnectionStrategy implements ReconnectionStrategy:
  static RECEIVE_CONNECT_TIME_OUT_SECONDS_ /int ::= 5
  static ATTEMPT_DELAYS_MS_ /List ::= [1_000, 5_000, 15_000]

  is_closed /bool := false

  connect transport/ActivityMonitoringTransport [--send_connect] [--receive_connect_ack] -> any:
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

/**
An MQTT client.

The client is responsible for maintaining the connection to the server.
If necessary it reconnects to the server.

When the connection to the broker is established with the clean-session bit, the client
  resubscribes all its subscriptions. However, due to the bit, there might be some
  messages that are lost.
*/
class Client:
  // TODO(florian): this should probably be part of the connection manager or
  // the transport.
  SEND_DISCONNECT_TIME_OUT_SECONDS_ /int ::= 5

  /** The client has been created. Handle has not been called yet. */
  static STATE_CREATED_ ::= 0
  /**
  The client is in the process of connecting.
  */
  static STATE_CONNECTING_ ::= 1
  /**
  The client is (or was) connected.
  This is a necesarry precondition for calling $handle.
  */
  static STATE_CONNECTED_ ::= 2
  /** The client is handling incoming packets. */
  static STATE_HANDLING_ ::= 3
  /**
  The client is disconnected and in the process of shutting down.
  This only happens once the current message has been handled. That is, once
    the $handle method's block has returned.
  */
  static STATE_CLOSING_ ::= 4
  /** The client is closed. */
  static STATE_CLOSED_ ::= 5

  state_ /int := STATE_CREATED_

  options_ /ClientOptions
  transport_ /ActivityMonitoringTransport
  logger_ /log.Logger?

  connection_ /Connection_? := null
  connecting_ /monitor.Mutex := monitor.Mutex

  // TODO(florian): move this into `Session_`.
  subscriptions_ /Map := {:}
  next_packet_id_/int? := 1
  pending_ / Map ::= {:}  // int -> Packet

  closed_ /Barrier_ ::= Barrier_

  /**
  A mutex to queue the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex

  reconnection_strategy_ /ReconnectionStrategy
  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $handle to initiate the connection.
  */
  constructor
      --options/ClientOptions
      --transport/Transport
      --logger/log.Logger?
      --reconnection_strategy /ReconnectionStrategy = DefaultReconnectionStrategy:
    options_ = options
    transport_ = ActivityMonitoringTransport.private_(transport)
    logger_ = logger
    reconnection_strategy_ = reconnection_strategy

  connect -> none:
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_
    state_ = STATE_CONNECTING_
    reconnect_ --reason=null
    state_ = STATE_CONNECTED_

  handle --on_packet/Lambda -> none:
    if state_ != STATE_CONNECTED_: throw "INVALID_STATE"
    state_ = STATE_HANDLING_

    try:
      while true:
        packet /Packet? := null
        do_connected_: packet = connection_.read
        if not packet: break

        if packet is PublishPacket:
          on_packet.call packet
        else if packet is ConnAckPacket:
          if logger_: logger_.info "spurious conn-ack packet"
        else if packet is PingRespPacket:
          // Ignore.
        else if packet is PacketIDAck:
          ack := packet as PacketIDAck
          id := ack.packet_id
          // TODO(florian): implement persistence layer.
          pending_.remove id
              --if_absent=: logger_.info "unmatched packet id: $id"
        else:
          if logger_: logger_.info "unexpected packet of type $packet.type"
    finally: | is_exception exception |
      // Exceptions in the callback bring down the Mqtt client.
      // This is harsh, but we don't want to lose messages.
      if is_exception:
        reconnection_strategy_.close
      tear_down_
      connection_ = null

  do_connected_ [block]:
    if is_closing or is_closed: throw CLIENT_CLOSED_EXCEPTION
    while true:
      // If the connection is still alive, or if the manager doesn't want us to reconnect, let the
      // exception go through.
      should_abandon := :
        connection_.is_alive or not reconnection_strategy_.should_try_reconnect transport_

      exception := catch --unwind=should_abandon:
        block.call
        return
      assert: exception != null
      if is_closing or is_closed: throw CLIENT_CLOSED_EXCEPTION
      reconnect_ --reason=exception
      if is_closing or is_closed: throw CLIENT_CLOSED_EXCEPTION

  reconnect_ --reason -> none:
    assert: not connection_ or not connection_.is_alive
    old_connection := connection_

    connecting_.do:
      // Check that nobody else created the connection in the meantime.
      if connection_ != old_connection: return
      try:
        connection_ = Connection_ transport_ --keep_alive=options_.keep_alive
        response := reconnection_strategy_.connect transport_
            --send_connect = :
              packet := ConnectPacket options_.client_id
                  --username=options_.username
                  --password=options_.password
                  --keep_alive=options_.keep_alive
                  --last_will=options_.last_will
              connection_.write packet
            --receive_connect_ack = : connection_.read
        if not response: throw "INTERNAL_ERROR"
        if is_closed: throw CLIENT_CLOSED_EXCEPTION
        ack := (response as ConnAckPacket)
        return_code := ack.return_code
        if return_code != 0:
          refused_reason := "CONNECTION_REFUSED"
          if return_code == 1: refused_reason = "UNACCEPTABLE_PROTOCOL_VERSION"
          if return_code == 2: refused_reason = "IDENTIFIER_REJECTED"
          if return_code == 3: refused_reason = "SERVER_UNAVAILABLE"
          if return_code == 4: refused_reason = "BAD_USERNAME_OR_PASSWORD"
          if return_code == 5: refused_reason = "NOT_AUTHORIZED"

          connection_.close --reason=refused_reason
          // No need to retry.
          close --no-disconnect
          throw refused_reason

        // Make sure the connection sends pings so the broker doesn't drop us.
        connection_.keep_alive --background

        // Before publishing messages, we need to subscribe to the topics we were subscribed to.
        // TODO(florian): also send/clear pending acks?
        if not subscriptions_.is_empty:
          topic_list := []
          subscriptions_.do: | topic/string max_qos/int |
            topic_list.add (TopicFilter topic --max_qos=max_qos)
          subscribe_all topic_list
          packet_id := next_packet_id_++
          subscribe_packet := SubscribePacket topic_list --packet_id=packet_id
          connection_.write subscribe_packet
          // TODO(florian): the subscription here is qos=1, but we don't want to resend it
          // if the connection breaks. (Or at least not in the usual way.)
          pending_[packet_id] = subscribe_packet

      finally: | is_exception _ |
        if is_exception:
          connection_.close
          close

  /**
  Tears down the client.
  */
  tear_down_:
    critical_do:
      state_ = STATE_CLOSED_
      if connection_:
        connection_.close
        connection_ = null
      closed_.set true

  /**
  Closes the client.

  If $disconnect is true and the connection is still alive send a disconnect packet,
    informing the broker of the disconnection.
  */
  close --disconnect/bool=true:
    if is_closing or is_closed: return

    if state_ == STATE_CREATED_ or state_ == STATE_CONNECTING_ or state_ == STATE_CONNECTED_:
      tear_down_
      return

    assert: state_ == STATE_HANDLING_

    state_ = STATE_CLOSING_

    // Note that disconnect packets don't need a packet id (which is important as
    // the packet_id counter is used as marker that the client is closed).
    if connection_.is_alive:
      if disconnect:
        catch:
          with_timeout --ms=SEND_DISCONNECT_TIME_OUT_SECONDS_ * 1000:
            connection_.write DisconnectPacket

      // The connection disconnect will stop any receiving in the $handle method.
      // This, in turn, will invoke the `tear_down` in $handle method.
      connection_.close

  /**
  Whether the client is closing.

  After a call to `close` (internal or external), the client switches to a closing state.
  It is only considered fully closed when $handle returns.
  */
  is_closing -> bool:
    return state_ == STATE_CLOSING_

  /** Whether the client is closed. */
  is_closed -> bool:
    return state_ == STATE_CLOSED_

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
    // The client is only active once $start has been called.
    if state_ == STATE_CREATED_: throw "INVALID_STATE"
    if is_closing or is_closed: throw CLIENT_CLOSED_EXCEPTION
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
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    if topic_filters.is_empty: return

    topic_filters.do: | topic_filter/TopicFilter |
      subscriptions_[topic_filter.filter] = topic_filter.max_qos

    packet_id := next_packet_id_++
    packet := SubscribePacket topic_filters --packet_id=packet_id
    send_ packet --packet_id=packet_id

  /** Unsubscribes from a single topic $filter. */
  unsubscribe filter/string -> none:
    unsubscribe_all [filter]

  /** Unsubscribes from the list of topic $filters (of type $string). */
  unsubscribe_all filters/List -> none:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    if filters.is_empty: return

    packet_id := next_packet_id_++
    packet := UnsubscribePacket filters --packet_id=packet_id
    send_ packet --packet_id=packet_id

  send_ packet/Packet --packet_id/int? -> none:
    sending_.do:
      if is_closing or is_closed: throw CLIENT_CLOSED_EXCEPTION
      if packet is ConnectPacket: throw "INVALID_PACKET"
      do_connected_: connection_.write packet
      if packet_id: pending_[packet_id] = packet

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
        do_connected_: connection_.write ack
