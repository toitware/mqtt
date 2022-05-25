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
  It throws the original exception. The other side simply throws a $CLIENT_CLOSED_EXCEPTION.

Also sends ping requests to keep the connection alive (if $keep_alive is called).
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
    if activity_task_: throw "ALREADY_RUNNING"
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
        close --reason=exception.value
        state_ = STATE_CLOSED_

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
          close --reason=exception.value

  close --reason=null:
    if is_closed: return
    assert: closing_reason_ == null
    critical_do:
      closing_reason_ = reason
      // By setting the state to closed we quell any error messages from disconnecting the transport.
      state_ = STATE_CLOSED_
      if activity_task_:
        activity_task_.cancel
        activity_task_ = null
      catch: transport_.close

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

  /** Whether the strategy is closed. */
  is_closed -> bool

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
      // A clean-session strategy must not find an existing session.
      throw "INVALID_STATE"

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return false

class DefaultSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:
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
    session_exists := do_connect transport
        --reconnect_transport = reconnect_transport
        --send_connect = send_connect
        --receive_connect_ack = receive_connect_ack

    if is_initial_connection or session_exists: return

    // The session was reset.
    // Disconnect and throw. If the user wants to, they should create a new client.
    catch: disconnect.call
    throw "SESSION_EXPIRED"

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return transport.supports_reconnect

  is_closed -> bool:
    return is_closed_

  close -> none:
    is_closed_ = true

class Session_:
  next_packet_id_ /int? := 1
  pending_ /Map ::= {:}  // From packet_id to persistent_id.

  set_pending_ack --packet_id/int --persistent_id/int:
    pending_[packet_id] = persistent_id

  handle_ack id/int [--if_absent] -> int?:
    result := pending_.get id
    pending_.remove id --if_absent=if_absent
    return result

  remove_pending id/int -> none:
    pending_.remove id

  next_packet_id -> int:
    return next_packet_id_++

  /**
  Runs over the pending packets.

  It is not allowed to change the pending map while calling this method.

  Calls the $block with the packet_id and persistent_id of each pending packet.
  */
  do --pending/bool [block] -> none:
    pending_.do block

/**
A persistence strategy for the MQTT client.

Note that the client does not automatically resend old messages when it starts up.
This is also true if the client tries to connect reusing an existing session, but
  the session has expired. In these cases, the user must manually resend the old
  messages.
*/
interface PersistenceStore:
  store topic/string payload/ByteArray --retain/bool -> int

  /**
  Finds the persistent packet with $packet_id and calls the given $block.

  The store may decide not to resend a packet, in which case it calls
    $if_absent with the $packet_id.
  */
  get packet_id/int [block] [--if_absent] -> none
  remove packet_id/int -> none
  /**
  Calls the given block for each stored packet.

  The arguments to the block are:
  - the persistent id
  - the topic
  - the payload
  - the retain flag
  */
  do [block] -> none

class PersistentPacket_:
  topic /string
  payload /ByteArray
  retain /bool

  constructor .topic .payload --.retain:

class MemoryPersistenceStore implements PersistenceStore:
  storage_ /Map := {:}
  id_ /int := 0

  store topic/string payload/ByteArray --retain/bool -> int:
    id := id_++
    storage_[id] = PersistentPacket_ topic payload --retain=retain
    return id

  get packet_id/int [block] [--if_absent] -> none:
    stored := storage_.get packet_id
    if stored:
      block.call stored.topic stored.payload stored.retain
    else:
      if_absent.call packet_id

  remove packet_id/int -> none:
    storage_.remove packet_id

  /**
  Calls the given block for each stored packet.

  The arguments to the block are:
  - the persistent id
  - the topic
  - the payload
  - the retain flag
  */
  do [block] -> none:
    storage_.do: | id/int packet/PersistentPacket_ |
      block.call id packet.topic packet.payload packet.retain

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
  The client has disconnected.
  The packet might not be sent yet (if other packets are queued in front), but no
    calls to $publish, ... should be done.
  */
  static STATE_DISCONNECTED_ ::= 4
  /**
  The client is disconnected and in the process of shutting down.
  This only happens once the current message has been handled. That is, once
    the $handle method's block has returned.
  Externally, the client class considers this to be equivalent to being closed. The
    $is_closed method returns true when the $state_ is set to $STATE_CLOSING_.
  */
  static STATE_CLOSING_ ::= 5
  /** The client is closed. */
  static STATE_CLOSED_ ::= 6

  state_ /int := STATE_CREATED_

  options_ /SessionOptions
  transport_ /ActivityMonitoringTransport := ?
  logger_ /log.Logger?

  session_ /Session_? := null

  connection_ /Connection_? := null
  connecting_ /monitor.Mutex := monitor.Mutex

  /**
  Latch that is set when the $handle method is run. This indicates that the
    client is running.

  Note that we allow to read the latch multiple times (which is currently not
    allowed according to its documentation).
  */
  handling_latch_ /monitor.Latch := monitor.Latch

  /**
  A mutex to queue the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex

  reconnection_strategy_ /ReconnectionStrategy

  /**
  The persistence store used by this client.

  All messages with qos=1 are stored in this store immediately after they have
    been sent. Once the client receives an 'ack' they are removed from it.

  If the client closes, but the store isn't empty, then some messages might not
    have reached the broker.

  Use $PersistenceStore.do to iterate over the stored messages.
  */
  persistence_store /PersistenceStore

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $connect, followed by $handle to initiate
    the connection.
  */
  constructor
      --options   /SessionOptions
      --transport /Transport
      --logger    /log.Logger?
      --persistence_store /PersistenceStore? = null
      --reconnection_strategy /ReconnectionStrategy? = null:
    options_ = options
    transport_ = ActivityMonitoringTransport.private_(transport)
    logger_ = logger
    this.persistence_store = persistence_store or MemoryPersistenceStore
    if reconnection_strategy:
      reconnection_strategy_ = reconnection_strategy
    else if options.clean_session:
      reconnection_strategy_ = DefaultCleanSessionReconnectionStrategy
    else:
      reconnection_strategy_ = DefaultSessionReconnectionStrategy

  /**
  Checks whether a user is allowed to send a message.

  The client must have connected, and run the $handle loop.
  */
  check_allowed_to_send_:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    // The client is only active once $start has been called.
    if not is_running: throw "INVALID_STATE"

  connect -> none:

    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_

    session_ = Session_

    state_ = STATE_CONNECTING_
    reconnect_ --is_initial_connection
    state_ = STATE_CONNECTED_

  handle --on_packet/Lambda -> none:
    if state_ != STATE_CONNECTED_: throw "INVALID_STATE"
    state_ = STATE_HANDLING_

    try:
      handling_latch_.set true
      while true:
        packet /Packet? := null
        do_connected_: packet = connection_.read
        if not packet:
          // Normally the broker should only close the connection when we have
          // sent a disconnect. However, we are also ok with being in a closed state.
          // In theory this could hide unexpected disconnects from the server, but it's
          // much more likely that we first disconnected, and then called $close for
          // another reason (like timeout).
          if state_ != STATE_DISCONNECTED_ and state_ != STATE_CLOSED_:
            throw reader.UNEXPECTED_END_OF_READER_EXCEPTION
          // Gracefully shut down.
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
          // The persistence store is allowed to toss out packets it doesn't want to
          // send again. If we receive an unmatched packet id, it might be from an
          // earlier attempt to send it.
          persistence_id := session_.handle_ack id
              --if_absent=: logger_.info "unmatched packet id: $id"
          if persistence_id: persistence_store.remove persistence_id
        else:
          if logger_: logger_.info "unexpected packet of type $packet.type"
    finally:
      tear_down_

  when_running [block] -> none:
    handling_latch_.get

  /**
  Closes the client.

  Unless $force is true, just sends a disconnect packet to the broker. The client then
    shuts down gracefully once the broker has closed the connection.

  If $force is true, shuts down the client by severing the transport.

  # Disconnect
  In the case of a graceful (non-$force close), the following applies:

  If other tasks are currently sending packets (with $publish or (un)$subscribe), then
    waits for these tasks to finish, before taking any action.

  If the client has already sent a disconnect packet, or is closed, does nothing.

  Never tries to reconnect. If the connection is not alive when it's time to send the
    disconnect packet, does nothing.

  # Forced shutdown
  In the case of a forced shutdown, calls $Transport.close on the current transport.

  No further packet is sent or received at this point.

  If the client is handling a callback, it still waits for it to finish before
    returning from $handle, at which point the client is considered fully closed.
  */
  close --force/bool=false -> none:
    if state_ == STATE_CLOSED_ or state_ == STATE_CLOSING_: return

    // If the $handle method hasn't been called, just tear down the client.
    if state_ == STATE_CREATED_ or state_ == STATE_CONNECTING_ or state_ == STATE_CONNECTED_:
      tear_down_
      return

    if not force: disconnect_
    else: close_force_

  disconnect_ -> none:
    if state_ == STATE_DISCONNECTED_: return

    check_allowed_to_send_

    state_ = STATE_DISCONNECTED_

    sending_.do:
      if state_ == STATE_CLOSING_ or state_ == STATE_CLOSED_ : return

      // This shouldn't be necessary, as we don't go through the $do_connected_ method.
      reconnection_strategy_.close

      if connection_.is_alive:
        connection_.write DisconnectPacket

  /**
  Forcefully closes the client.
  */
  close_force_ -> none:
    assert: state_ == STATE_HANDLING_
    // Since we are in a handling state, there must have been a $connect call, and as such
    // a reconnection_strategy
    assert: reconnection_strategy_

    state_ = STATE_CLOSING_

    reconnection_strategy_.close

    // Shut down the connection, which will lead to the $handle method returning
    //   which will then tear down the client.
    connection_.close

  /**
  Whether the client is disconnected, closing or closed.

  After a call to disconnect, or a call to $close, the client starts to shut down.
  However, it is only considered to be fully closed when the $handle method returns.
  */
  is_closed -> bool:
    return state_ == STATE_DISCONNECTED_ or state_ == STATE_CLOSING_ or state_ == STATE_CLOSED_

  /**
  Whether the client is connected and running.

  If true, users are allowed to send messages or change subscriptions.
  */
  is_running -> bool:
    return state_ == STATE_HANDLING_

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
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet_id := qos > 0 ? session_.next_packet_id : null

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet_id=packet_id

    send_ packet
    if qos == 1:
      persistent_id := persistence_store.store topic payload --retain=retain
      session_.set_pending_ack --packet_id=packet_id --persistent_id=persistent_id

  /**
  Subscribes to the given $filter with a max qos of $max_qos.

  Returns the packet id of the subscribe packet.
  */
  subscribe filter/string --max_qos/int=1 -> int:
    return subscribe_all [ TopicFilter filter --max_qos=max_qos ]

  /**
  Subscribes to the given list $topic_filters of type $TopicFilter.

  Returns the packet id of the subscribe packet or -1 if the $topic_filters is empty.
  */
  subscribe_all topic_filters/List -> int:
    if topic_filters.is_empty: return -1

    packet_id := session_.next_packet_id
    packet := SubscribePacket topic_filters --packet_id=packet_id
    send_ packet
    return packet_id

  /**
  Unsubscribes from a single topic $filter.

  Returns the packet id of the unsubscribe packet.
  */
  unsubscribe filter/string -> int:
    return unsubscribe_all [filter]

  /**
  Unsubscribes from the list of topic $filters (of type $string).

  Returns the packet id of the unsubscribe packet or -1 if the $filters is empty.
  */
  unsubscribe_all filters/List -> int:
    if filters.is_empty: return -1

    packet_id := session_.next_packet_id
    packet := UnsubscribePacket filters --packet_id=packet_id
    send_ packet
    return packet_id

  /**
  Acknowledges the hand-over of the packet.

  If the packet has qos=1, sends an ack packet to the broker.

  If the client isn't running, does nothing.
  */
  ack packet/Packet:
    // Can't ack if we don't have a connection anymore.
    // Don't use $is_closed, as we are allowed to send acks after a $disconnect.
    if state_ == STATE_CLOSING_ or state_ == STATE_CLOSED_: return
    if state_ != STATE_HANDLING_ and state_ != STATE_DISCONNECTED_: throw "INVALID_STATE"
    check_allowed_to_send_
    if packet is PublishPacket:
      id := (packet as PublishPacket).packet_id
      if id:
        ack := PubAckPacket id
        // Skip the 'sending_' queue and write directly to the connection.
        // This way ack-packets are transmitted faster.
        do_connected_: connection_.write ack

  send_ packet/Packet -> none:
    if packet is ConnectPacket: throw "INVALID_PACKET"
    check_allowed_to_send_
    sending_.do:
      // While waiting in the queue the client could have been closed.
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      do_connected_: connection_.write packet

  do_connected_ [block]:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    while true:
      // If the connection is still alive, or if the manager doesn't want us to reconnect, let the
      // exception go through.
      should_abandon := :
        connection_.is_alive or reconnection_strategy_.is_closed or
          not reconnection_strategy_.should_try_reconnect transport_

      exception := catch --unwind=should_abandon --trace:
        block.call
        return
      assert: exception != null
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      reconnect_ --is_initial_connection=false
      if is_closed: throw CLIENT_CLOSED_EXCEPTION

  refused_reason_for_return_code_ return_code/int -> string:
    refused_reason := "CONNECTION_REFUSED"
    if return_code == 1: refused_reason = "UNACCEPTABLE_PROTOCOL_VERSION"
    else if return_code == 2: refused_reason = "IDENTIFIER_REJECTED"
    else if return_code == 3: refused_reason = "SERVER_UNAVAILABLE"
    else if return_code == 4: refused_reason = "BAD_USERNAME_OR_PASSWORD"
    else if return_code == 5: refused_reason = "NOT_AUTHORIZED"
    return refused_reason

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
                refused_reason := refused_reason_for_return_code_ return_code
                connection_.close --reason=refused_reason
                // No need to retry.
                close --force
                throw refused_reason
              ack.session_present
            --disconnect = :
              connection_.write DisconnectPacket
      finally: | is_exception _ |
        if is_exception:
          close --force

      // If we are here, then the reconnection succeeded.

      // Make sure the connection sends pings so the broker doesn't drop us.
      connection_.keep_alive --background

      // Resend the pending messages.
      session_.do --pending: | packet_id/int persistent_id/int |
        // The persistence store is allowed to decide not to resend packets.
        persistence_store.get persistent_id
          --if_absent=: session_.remove_pending packet_id
          : | topic/string payload/ByteArray retain/bool |
            packet := PublishPacket topic payload --packet_id=packet_id --qos=1 --retain=retain --duplicate
            connection_.write packet

  /** Tears down the client. */
  tear_down_:
    critical_do:
      state_ = STATE_CLOSED_
      reconnection_strategy_.close
      connection_.close

