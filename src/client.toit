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

  keep_alive_duration_ /Duration?
  activity_task_ /Task_? := null

  /** Constructs a new connection. */
  constructor .transport_ --keep_alive/Duration?:
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
    print "closing connection"
    if is_closed: return
    assert: closing_reason_ == null
    critical_do:
      closing_reason_ = reason
      // By setting the state to closed we quell any error messages from disconnecting the transport.
      state_ = STATE_CLOSED_
      catch: transport_.close
      if activity_task_:
        activity_task_.cancel
        activity_task_ = null
    print "closing connection done"

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
          print "closing because of $exception.value"
          close --reason=exception

/**
A strategy to connect to the broker.

This class deals with (temporary) disconnections and timeouts.
It keeps track of whether trying to connect again makes sense, and when it
  should try again.

It is also called for the first time the client connects to the broker.
*/
interface ReconnectionStrategy:
  /**
  Is called when the client wants to establish a connection through the given $transport.

  The strategy should first call $send_connect, followed by a $receive_connect_ack. If
    the connection is unsuccessful, it may retry.

  The $receive_connect_ack block returns whether the broker had a session for this client.

  The $is_initial_connection is true if this is the first time the client connects to the broker.
  */
  connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]

  /** Whether the client should even try to reconnect. */
  should_try_reconnect transport/ActivityMonitoringTransport -> bool

  /** Closes the strategy, indicating that no further reconnection attempts should be done. */
  close -> none

abstract class DefaultReconnectionStrategyBase implements ReconnectionStrategy:
  static DEFAULT_RECEIVE_CONNECT_TIMEOUT /Duration ::= Duration --s=5
  static DEFAULT_ATTEMPT_DELAYS /List ::= [
    Duration --s=1,
    Duration --s=5,
    Duration --s=15,
  ]

  receive_connect_timeout_ /Duration
  attempt_delays_ /List

  is_closed_ := false

  constructor
      --receive_connect_timeout /Duration = DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --attempt_delays /List /*Duration*/ = DEFAULT_ATTEMPT_DELAYS:
    receive_connect_timeout_ = receive_connect_timeout
    attempt_delays_ = attempt_delays


  /**
  Tries to connect, potentially retrying with delays.

  Returns null if the strategy was closed.
  Returns whether the broker had a session for this client, otherwise.
  */
  do_connect transport/ActivityMonitoringTransport
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]:
    for i := -1; i < attempt_delays_.size; i++:
      if is_closed: return null
      if i >= 0:
        sleep attempt_delays_[i]
        if is_closed: return null
        reconnect_transport.call

      did_connect := false

      is_last_attempt := (i == attempt_delays_.size - 1)

      catch --unwind=(: is_closed or is_last_attempt):
        send_connect.call
        return with_timeout receive_connect_timeout_:
          receive_connect_ack.call

      if is_closed: return null

    unreachable

  abstract connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]

  abstract should_try_reconnect transport/ActivityMonitoringTransport -> bool

  is_closed -> bool:
    return is_closed_

  close -> none:
    is_closed_ = true


class DefaultCleanSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:

  constructor
      --receive_connect_timeout /Duration = DefaultReconnectionStrategyBase.DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --attempt_delays /List /*Duration*/ = DefaultReconnectionStrategyBase.DEFAULT_ATTEMPT_DELAYS:
    super --receive_connect_timeout=receive_connect_timeout --attempt_delays=attempt_delays

  connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]:
    // The clean session does not reconnect.
    if not is_initial_connection: throw "INVALID_STATE"
    session_exists := do_connect transport
        --reconnect_transport = reconnect_transport
        --send_connect = send_connect
        --receive_connect_ack = receive_connect_ack
    if session_exists:
      // A clean-session strategy can't find an existing session.
      throw "INVALID_STATE"

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return false

class DefaultSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:
  session_reset_callback_ /Lambda?

  constructor
      --receive_connect_timeout /Duration = DefaultReconnectionStrategyBase.DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --attempt_delays /List /*Duration*/ = DefaultReconnectionStrategyBase.DEFAULT_ATTEMPT_DELAYS
      --on_session_reset /Lambda? = null:
    session_reset_callback_ = on_session_reset
    super --receive_connect_timeout=receive_connect_timeout --attempt_delays=attempt_delays

  connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]:
    session_exists := do_connect transport
        --reconnect_transport = reconnect_transport
        --send_connect = send_connect
        --receive_connect_ack = receive_connect_ack

    if is_initial_connection or session_exists: return

    // The session was reset.
    if session_reset_callback_:
      session_reset_callback_.call
    else:
      disconnect.call
      throw "SESSION_EXPIRED"

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return transport.supports_reconnect

  is_closed -> bool:
    return is_closed_

  close -> none:
    is_closed_ = true

class Session_:
  subscriptions /Map ::= {:}
  next_packet_id_ /int? := 1
  pending_ /Map ::= {:}  // int -> Packet.

  set_pending_ack packet/Packet --packet_id/int:
    pending_[packet_id] = packet

  handle_ack id/int [--if_absent]:
    pending_.remove id --if_absent=if_absent

  next_packet_id -> int:
    return next_packet_id_++

  has_subscriptions -> bool:
    return not subscriptions.is_empty

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

  options_ /SessionOptions
  transport_ /ActivityMonitoringTransport := ?
  logger_ /log.Logger?

  session_ /Session_? := null

  connection_ /Connection_? := null
  connecting_ /monitor.Mutex := monitor.Mutex

  handling_latch_ /monitor.Latch := monitor.Latch

  /**
  A mutex to queue the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex

  reconnection_strategy_ /ReconnectionStrategy? := null

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $connect, followed by $handle to initiate
    the connection.
  */
  constructor
      --options   /SessionOptions
      --transport /Transport
      --logger    /log.Logger?:
    options_ = options
    transport_ = ActivityMonitoringTransport.private_(transport)
    logger_ = logger

  connect -> none
      --reconnection_strategy /ReconnectionStrategy =
          (options_.clean_session
            ? DefaultCleanSessionReconnectionStrategy
            : DefaultSessionReconnectionStrategy):
    reconnection_strategy_ = reconnection_strategy
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_
    session_ = Session_

    state_ = STATE_CONNECTING_
    print "connect"
    reconnect_ --is_initial_connection
    print "connect done"
    state_ = STATE_CONNECTED_

  handle --on_packet/Lambda -> none:
    print "handling"
    if state_ != STATE_CONNECTED_: throw "INVALID_STATE"
    state_ = STATE_HANDLING_

    try:
      handling_latch_.set true
      while true:
        packet /Packet? := null
        do_connected_: packet = connection_.read
        if not packet:
          if not is_closed: throw reader.UNEXPECTED_END_OF_READER_EXCEPTION
          break

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
          session_.handle_ack id
              --if_absent=: logger_.info "unmatched packet id: $id"
        else:
          if logger_: logger_.info "unexpected packet of type $packet.type"
    finally:
      if reconnection_strategy_: reconnection_strategy_.close
      tear_down_

  when_handling [block] -> none:
    handling_latch_.get

  /**
  Closes the client.

  If $disconnect is true and the connection is still alive send a disconnect packet,
    informing the broker of the disconnection.
  */
  close --disconnect/bool=true:
    if is_closed: return

    if state_ == STATE_CREATED_ or state_ == STATE_CONNECTING_ or state_ == STATE_CONNECTED_:
      tear_down_
      return

    assert: state_ == STATE_HANDLING_

    state_ = STATE_CLOSING_

    if reconnection_strategy_:
      reconnection_strategy_.close

    // Note that disconnect packets don't need a packet id (which is important as
    // the packet_id counter is used as marker that the client is closed).
    if connection_.is_alive:
      if disconnect:
        catch: connection_.write DisconnectPacket
        // TODO(florian): we should wait for the reader to return `null` before
        // killing the connection.


      // If there was a disconnect packet, then the broker should shut down the connection.
      // If that didn't work, or if we didn't send one, just shut the connection down now.
      // The connection disconnect will stop any receiving in the $handle method.
      // This, in turn, will invoke the `tear_down` in $handle method.
      connection_.close

  /**
  Whether the client is closing or closed.

  After a call to `close` (internal or external), the client starts to close.
  It is only considered fully closed when $handle returns.
  */
  is_closed -> bool:
    return state_ == STATE_CLOSING_ or state_ == STATE_CLOSED_

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
    print "publishing"
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    // The client is only active once $start has been called.
    if state_ != STATE_HANDLING_: throw "INVALID_STATE"
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet_id := qos > 0 ? session_.next_packet_id : null

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
    // The client is only active once $start has been called.
    if state_ != STATE_HANDLING_: throw "INVALID_STATE"

    if topic_filters.is_empty: return

    topic_filters.do: | topic_filter/TopicFilter |
      session_.subscriptions[topic_filter.filter] = topic_filter.max_qos

    packet_id := session_.next_packet_id
    packet := SubscribePacket topic_filters --packet_id=packet_id
    send_ packet --packet_id=packet_id

  /** Unsubscribes from a single topic $filter. */
  unsubscribe filter/string -> none:
    unsubscribe_all [filter]

  /** Unsubscribes from the list of topic $filters (of type $string). */
  unsubscribe_all filters/List -> none:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    // The client is only active once $start has been called.
    if state_ != STATE_HANDLING_: throw "INVALID_STATE"

    if filters.is_empty: return

    packet_id := session_.next_packet_id
    packet := UnsubscribePacket filters --packet_id=packet_id
    send_ packet --packet_id=packet_id

  /**
  Acknowledges the hand-over of the packet.

  If the packet has qos=1, sends an ack packet to the broker.
  */
  ack packet/Packet:
    // Can't ack if we don't have a connection anymore.
    if is_closed: return
    if packet is PublishPacket:
      id := (packet as PublishPacket).packet_id
      if id:
        ack := PubAckPacket id
        do_connected_: connection_.write ack

  send_ packet/Packet --packet_id/int? -> none:
    sending_.do:
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      if packet is ConnectPacket: throw "INVALID_PACKET"
      do_connected_: connection_.write packet
      if packet_id: session_.set_pending_ack packet --packet_id=packet_id

  do_connected_ [block]:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    while true:
      // If the connection is still alive, or if the manager doesn't want us to reconnect, let the
      // exception go through.
      should_abandon := :
        connection_.is_alive or not reconnection_strategy_.should_try_reconnect transport_

      exception := catch --unwind=should_abandon --trace:
        block.call
        return
      assert: exception != null
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      reconnect_ --is_initial_connection=false
      if is_closed: throw CLIENT_CLOSED_EXCEPTION

  reconnect_ --is_initial_connection/bool -> none:
    assert: not connection_ or not connection_.is_alive
    old_connection := connection_

    connecting_.do:
      // Check that nobody else reconnected while we took the lock.
      if connection_ != old_connection: return

      if not connection_:
        assert: is_initial_connection
        connection_ = Connection_ transport_ --keep_alive=options_.keep_alive

      try:
        reconnection_strategy_.connect transport_
            --is_initial_connection = is_initial_connection
            --reconnect_transport = :
              transport_.reconnect
              connection_ = Connection_ transport_ --keep_alive=options_.keep_alive
            --send_connect = :
              packet := ConnectPacket options_.client_id
                  --clean_session=options_.clean_session
                  --username=options_.username
                  --password=options_.password
                  --keep_alive=options_.keep_alive
                  --last_will=options_.last_will
              connection_.write packet
            --receive_connect_ack = :
              response := connection_.read
              if not response: throw "INTERNAL_ERROR"
              if is_closed: throw CLIENT_CLOSED_EXCEPTION
              ack := (response as ConnAckPacket)
              return_code := ack.return_code
              if return_code != 0:
                refused_reason := "CONNECTION_REFUSED"
                if return_code == 1: refused_reason = "UNACCEPTABLE_PROTOCOL_VERSION"
                else if return_code == 2: refused_reason = "IDENTIFIER_REJECTED"
                else if return_code == 3: refused_reason = "SERVER_UNAVAILABLE"
                else if return_code == 4: refused_reason = "BAD_USERNAME_OR_PASSWORD"
                else if return_code == 5: refused_reason = "NOT_AUTHORIZED"

                connection_.close --reason=refused_reason
                // No need to retry.
                close --no-disconnect
                throw refused_reason
              ack.session_present
            --disconnect = :
              close --disconnect

        // TODO(florian): resend pending messages if we have the old session.
        // Send them as dupes.

        // Make sure the connection sends pings so the broker doesn't drop us.
        connection_.keep_alive --background

      finally: | is_exception _ |
        if is_exception:
          connection_.close
          close

  /** Tears down the client. */
  tear_down_:
    critical_do:
      state_ = STATE_CLOSED_
      connection_.close

