// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor
import log
import reader

import .session_options
import .last_will
import .packets
import .tcp  // For toitdoc.
import .topic_qos
import .transport
import .util

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
      return keep_alive_

  run:
    while not Task.current.is_canceled and not connection_.is_closed:
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
  writer_ /Writer
  writing_ /monitor.Mutex ::= monitor.Mutex
  is_writing_ /bool := false

  closing_reason_ /any := null

  keep_alive_duration_ /Duration?
  // TODO(florian): the following field should be typed as `Task?`.
  // However, that class is only available in Toit 2.0.
  activity_task_ /any := null

  /** Constructs a new connection. */
  constructor .transport_ --keep_alive/Duration?:
    reader_ = reader.BufferedReader transport_
    writer_ = Writer transport_
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
    if keep_alive_duration_ == Duration.ZERO: return

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

  /** Whether the connection is in the process of writing. */
  is_writing -> bool:
    return is_writing_

  /**
  Writes the given $packet, serializing it first.
  */
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
        if exception:
          assert: is_closed
          throw CLIENT_CLOSED_EXCEPTION
      finally: | is_exception exception |
        is_writing_ = false
        if is_exception:
          close --reason=exception.value

  /**
  Closes the connection.

  If $reason is not null, the closing happens due to an error.
  Closes the underlying transport.
  */
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

/**
A latch that has a timeout when getting its value.
*/
monitor WaitSignal_:
  has_triggered_ /bool := false

  /**
  Either returns true if the signal is triggered or null if the timeout is reached.
  */
  wait --timeout/Duration -> bool?:
    try_await --deadline=(Time.monotonic_us + timeout.in_us): has_triggered_
    if has_triggered_: return true
    return null

  trigger -> none:
    has_triggered_ = true

/**
A base class for reconnection strategies.
*/
abstract class DefaultReconnectionStrategyBase implements ReconnectionStrategy:
  static DEFAULT_RECEIVE_CONNECT_TIMEOUT /Duration ::= Duration --s=5
  static DEFAULT_ATTEMPT_DELAYS /List ::= [
    Duration --s=1,
    Duration --s=5,
    Duration --s=15,
  ]

  receive_connect_timeout_ /Duration
  attempt_delays_ /List?
  delay_lambda_/Lambda?

  is_closed_ := false

  logger_/log.Logger

  /**
  A latch that is set when the client is closed.
  */
  closed_signal_ /WaitSignal_ := WaitSignal_

  constructor
      --receive_connect_timeout /Duration = DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --delay_lambda /Lambda? = null
      --attempt_delays /List? /*Duration*/ = (delay_lambda ? null : DEFAULT_ATTEMPT_DELAYS)
      --logger/log.Logger?=log.default:
    if delay_lambda and attempt_delays: throw "BOTH_DELAY_LAMBDAS_AND_DELAYS"
    receive_connect_timeout_ = receive_connect_timeout
    delay_lambda_ = delay_lambda
    attempt_delays_ = attempt_delays
    logger_ = logger

  /**
  Tries to connect, potentially retrying with delays.

  Returns null if the strategy was closed.
  Returns whether the broker had a session for this client, otherwise.
  */
  do_connect transport/ActivityMonitoringTransport
      --reuse_connection/bool=false
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]:
    attempt_counter := -1
    while true:
      if is_closed: return null

      attempt_counter++

      is_last_attempt := false
      if attempt_delays_:
        is_last_attempt = (attempt_counter == attempt_delays_.size)

      try:
        catch --unwind=(: is_closed or is_last_attempt):

          if attempt_counter == 0:
            // In the first iteration we try to connect without delay.
            if not reuse_connection:
              logger_.debug "Attempting to reconnect"
              reconnect_transport.call
          else:
            sleep_duration/Duration := ?
            if attempt_delays_:
              sleep_duration = attempt_delays_[attempt_counter - 1]
            else:
              sleep_duration = delay_lambda_.call attempt_counter
            closed_signal_.wait --timeout=sleep_duration
            if is_closed: return null
            logger_.debug "Attempting to reconnect"
            reconnect_transport.call

          send_connect.call
          logger_.debug "Connected to broker"
          return with_timeout receive_connect_timeout_:
            result := receive_connect_ack.call
            logger_.debug "Connection established"
            result
      finally: | is_exception _ |
        if is_exception:
          logger_.debug "Reconnection attempt failed"

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
    closed_signal_.trigger

/**
The default reconnection strategy for clients that are connected with the
  clean session bit.

Since the broker will drop the session information this strategy won't reconnect
  when the connection breaks after it has connected. However, it will potentially
  try to connect multiple times for the initial connection.
*/
class DefaultCleanSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:

  constructor
      --logger/log.Logger=log.default
      --receive_connect_timeout /Duration = DefaultReconnectionStrategyBase.DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --delay_lambda /Lambda? = null
      --attempt_delays /List? /*Duration*/ = null:
    super --logger=logger
        --receive_connect_timeout=receive_connect_timeout
        --delay_lambda=delay_lambda
        --attempt_delays=attempt_delays

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
        --reuse_connection = is_initial_connection
        --reconnect_transport = reconnect_transport
        --send_connect = send_connect
        --receive_connect_ack = receive_connect_ack
    if session_exists:
      // A clean-session strategy must not find an existing session.
      throw "INVALID_STATE"

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return false

/**
The default reconnection strategy for clients that are connected without the
  clean session bit.

If the connection drops, the client tries to reconnect potentially multiple times.
*/
class DefaultSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:
  constructor
      --logger/log.Logger=log.default
      --receive_connect_timeout /Duration = DefaultReconnectionStrategyBase.DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --delay_lambda /Lambda? = null
      --attempt_delays /List? = null:
    super --logger=logger
        --receive_connect_timeout=receive_connect_timeout
        --delay_lambda=delay_lambda
        --attempt_delays=attempt_delays

  connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]:
    session_exists := do_connect transport
        --reuse_connection = is_initial_connection
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

/**
A reconnection strategy that keeps reconnecting.

This strategy also ignores whether the broker did or did not have a session for
  the client. See below for the consequences of ignoring session state.

The strategy will keep trying to reconnect until the client is closed. The
  delay between each connection attempt can be configured by a lambda.

# Session state
If the client reconnects, but the broker doesn't have a session for the client,
  then this strategy continues as if nothing ever happened. This can, in
  theory, lead to a situation where the client acknowledges messages that the
  broker never sent. In rare cases, the client might even acknowledge a message
  that happens to have an ID of a packet the broker just sent, but the client
  hasn't received yet.
  Theoretically, this is a protocol violation, but most brokers just drop the packet.

Note that this can also happen to clients that don't set the clien-session flag, but
  where the broker lost the session. This can happen because the session expired, the
  broker crashed, or a client with the same ID connectend in the meantime with a
  clean-session flag.
*/
class TenaciousReconnectionStrategy extends DefaultReconnectionStrategyBase:
  /**
  Creates a new tenacious reconnection strategy.

  The given $delay_lambda must return a Duration, representing the delay between
    the next connection attempt. The first reconnection attempt is always
    immediate, so this lambda is only called before the second and subsequent
    attempts.

  The $receive_connect_timeout is the timeout given for the CONNECT packet to
    arrive at the client.
  */
  constructor
      --logger/log.Logger=log.default
      --receive_connect_timeout /Duration = DefaultReconnectionStrategyBase.DEFAULT_RECEIVE_CONNECT_TIMEOUT
      --delay_lambda /Lambda:
    super --logger=logger
        --receive_connect_timeout=receive_connect_timeout
        --delay_lambda=delay_lambda

  connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]:
    session_exists := do_connect transport
        --reuse_connection = is_initial_connection
        --reconnect_transport = reconnect_transport
        --send_connect = send_connect
        --receive_connect_ack = receive_connect_ack

    // We don't care if the session exists or not.
    // This strategy just wants to reconnect.
    return

  should_try_reconnect transport/ActivityMonitoringTransport -> bool:
    return transport.supports_reconnect

/**
An MQTT session.

Keeps track of data that must survive disconnects (assuming the user didn't use
  the 'clean_session' flag).
*/
class Session_:
  next_packet_id_ /int? := 1

  /**
  The persistence store used by this session.

  All messages with qos=1 are stored in this store immediately after they have
    been sent. Once the client receives an 'ack' they are removed from it.

  If the client closes, but the store isn't empty, then some messages might not
    have reached the broker.

  Use $PersistenceStore.do to iterate over the stored messages.
  */
  persistence_store_ /PersistenceStore

  options /SessionOptions

  constructor .options .persistence_store_:

  set_pending_ack topic/string payload/ByteArray --packet_id/int --retain/bool:
    persistent := PersistedPacket topic payload --retain=retain --packet_id=packet_id
    persistence_store_.add persistent

  handle_ack id/int [--if_absent] -> none:
    if not persistence_store_.remove_persisted_with_id id:
      if_absent.call id

  remove_pending id/int -> none:
    persistence_store_.remove_persisted_with_id id

  next_packet_id -> int:
    while true:
      candidate := next_packet_id_
      next_packet_id_ = (candidate + 1) & ((1 << PublishPacket.ID_BIT_SIZE) - 1)
      // If we are waiting for an ACK of the candidate id, pick a different one.
      if persistence_store_.get candidate: continue
      return candidate

  /**
  Runs over the pending packets.

  It is not allowed to change the pending map while calling this method.

  Calls the $block with the packet_id and persisted_id of each pending packet.
  */
  do --pending/bool [block] -> none:
    persistence_store_.do block

/**
A persistence strategy for the MQTT client.

Note that the client does *not* automatically resend old messages when it starts up.
This is also true if the client tries to connect reusing an existing session, but
  the session has expired. In these cases, the user must manually resend the old
  messages.
*/
interface PersistenceStore:
  /**
  Stores the publish-packet information in the persistent store.
  */
  add packet/PersistedPacket -> none

  /**
  Finds the persistent packet with $packet_id and returns it.

  If no such packet exists, returns null.

  # Advanced
  The MQTT protocol does not explicitly allow to drop packets. However,
    most brokers should do fine with it.
  */
  get packet_id/int -> PersistedPacket?

  /**
  Removes the data for the packet with $packet_id.
  Does nothing if there is no data associated to $packet_id.

  Returns whether the store contained the id.
  */
  remove_persisted_with_id packet_id/int -> bool

  /**
  Calls the given block for each stored packet.

  The order of the packets is in insertion order.

  The block is called with a $PersistedPacket.

  # Inheritance
  The MQTT protocol does not allow to drop messages that are not acknowledged.
    However, most brokers can probably deal with it.
  */
  do [block] -> none

class PersistedPacket:
  packet_id /int
  topic /string
  payload /ByteArray
  retain /bool

  constructor .topic .payload --.packet_id --.retain:

/**
A persistence store that stores the packets in memory.
*/
class MemoryPersistenceStore implements PersistenceStore:
  /**
  A map from packet id to $PersistedPacket.
  */
  storage_ /Map := {:}

  add packet/PersistedPacket -> none:
    id := packet.packet_id
    storage_[id] = packet

  get packet_id/int -> PersistedPacket?:
    return storage_.get packet_id

  remove_persisted_with_id packet_id/int -> bool:
    storage_.remove packet_id --if_absent=: return false
    return true

  /**
  Calls the given block for each stored packet in insertion order.
  See $PersistenceStore.do.
  */
  do [block] -> none:
    storage_.do --values block

  /** Whether no packet is persisted. */
  is_empty -> bool:
    return storage_.is_empty

  /**
  The count of persisted packets.
  */
  size -> int:
    return storage_.size

/**
An MQTT client.

The client is responsible for maintaining the connection to the server.
If necessary it reconnects to the server.

When the connection to the broker is established with the clean-session bit, the client
  resubscribes all its subscriptions. However, due to the bit, there might be some
  messages that are lost.
*/
class FullClient:
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

  transport_ /ActivityMonitoringTransport := ?
  logger_ /log.Logger

  session_ /Session_? := null
  reconnection_strategy_ /ReconnectionStrategy? := null

  connection_ /Connection_? := null
  connecting_ /monitor.Mutex := monitor.Mutex

  /**
  Latch that is set when the $handle method is run. This indicates that the
    client is running.

  Note that we allow to read the latch multiple times.
  */
  handling_latch_ /Latch := Latch

  /**
  A mutex to queue the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex

  /**
  Keeps track of the last acked packet.

  If the packet was not acked during the callback of $handle we will do so automatically
    when the callback returns.
  */
  unacked_packet_ /Packet? := null

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $connect, followed by $handle to initiate
    the connection.
  */
  constructor
      --transport /Transport
      --logger /log.Logger = log.default:
    transport_ = ActivityMonitoringTransport.private_ transport
    logger_ = logger

  /**
  Checks whether a user is allowed to send a message.

  The client must have connected, and run the $handle loop.
  */
  check_allowed_to_send_:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    // The client is only active once $handle has been called.
    if not is_running: throw "INVALID_STATE"

  /**
  Connects the client to the broker.

  Once the client is connected, the user should call $handle to handle incoming
    packets. Only, when the handler is active, can messages be sent.
  */
  connect -> none
      --options /SessionOptions
      --persistence_store /PersistenceStore = MemoryPersistenceStore
      --reconnection_strategy /ReconnectionStrategy? = null:
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_

    if reconnection_strategy:
      reconnection_strategy_ = reconnection_strategy
    else if options.clean_session:
      reconnection_strategy_ = DefaultCleanSessionReconnectionStrategy
          --logger=logger_.with_name "reconnect"
    else:
      reconnection_strategy_ = DefaultSessionReconnectionStrategy
          --logger=logger_.with_name "reconnect"

    session_ = Session_ options persistence_store

    state_ = STATE_CONNECTING_
    reconnect_ --is_initial_connection
    state_ = STATE_CONNECTED_

  /**
  Runs the receiving end of the client.

  The client is not considered started until this method has been called.
  This method is blocking.

  The given $on_packet block is called for each received packet.
    The block should $ack the packet as soon as possible.

  The $when_running method executes a given block when this method is running.
  */
  handle [on_packet] -> none:
    if state_ != STATE_CONNECTED_: throw "INVALID_STATE"
    state_ = STATE_HANDLING_

    try:
      handling_latch_.set true
      while not Task.current.is_canceled:
        packet /Packet? := null
        catch --unwind=(: not state_ == STATE_DISCONNECTED_ and not state_ == STATE_CLOSING_):
          do_connected_ --allow_disconnected: packet = connection_.read

        if not packet:
          // Normally the broker should only close the connection when we have
          // sent a disconnect. However, we are also ok with being in a closed state.
          // In theory this could hide unexpected disconnects from the server, but it's
          // much more likely that we first disconnected, and then called $close for
          // another reason (like timeout).
          if not is_closed:
            logger_.info "disconnect from server"
            // Reset the connection and try again.
            // The loop will call `do_connected_` which will ensure that we reconnect if
            // that's possible.
            connection_.close
            continue

          // The user called 'close'. Doesn't mean it was always completely graceful, but
          // good enough.
          break

        if packet is PublishPacket or packet is SubAckPacket or packet is UnsubAckPacket:
          unacked_packet_ = packet
          try:
            on_packet.call packet
            packet.ensure_drained_
            if is_running and unacked_packet_: ack unacked_packet_
          finally:
            unacked_packet_ = null
        else if packet is ConnAckPacket:
          logger_.info "spurious conn-ack packet"
        else if packet is PingRespPacket:
          // Ignore.
        else if packet is PubAckPacket:
          ack := packet as PubAckPacket
          id := ack.packet_id
          // The persistence store is allowed to toss out packets it doesn't want to
          // send again. If we receive an unmatched packet id, it might be from an
          // earlier attempt to send it.
          session_.handle_ack id
              --if_absent=: logger_.info "unmatched packet id: $id"
        else:
          logger_.info "unexpected packet of type $packet.type"
    finally:
      tear_down_

  /**
  Calls the given $block when the client is running.
  If the client was not able to connect, then the block is not called.
  */
  when_running [block] -> none:
    if is_closed: return
    is_running := handling_latch_.get
    if not is_running: return
    block.call

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
    disconnect packet, closes the client as if called with $force.

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

    state_ = STATE_DISCONNECTED_

    // We might be calling close from within the reconnection-strategy's callback.
    // If the connection is currently not alive simply force-close.
    if not connection_.is_alive:
      close_force_
      return

    reconnection_strategy_.close

    sending_.do:
      if state_ == STATE_CLOSING_ or state_ == STATE_CLOSED_ : return

      // Make sure we take the same lock as the reconnecting function.
      // Otherwise we might not send a disconnect, even though we just reconnected (or are
      // reconnecting).
      connecting_.do:
        if connection_.is_alive:
          connection_.write DisconnectPacket
        else:
          close_force_

  /**
  Forcefully closes the client.
  */
  close_force_ -> none:
    assert: state_ == STATE_HANDLING_ or state_ == STATE_DISCONNECTED_
    // Since we are in a handling/disconnected state, there must have been a $connect call, and as such
    // a reconnection_strategy
    assert: session_

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
  publish topic/string payload/ByteArray --qos/int=1 --retain/bool=false -> none:
    if topic == "" or topic.contains "+" or topic.contains "#": throw "INVALID_ARGUMENT"
    packet_id := send_publish_ topic payload --qos=qos --retain=retain
    if qos == 1:
      session_.set_pending_ack topic payload --packet_id=packet_id --retain=retain

  /**
  Sends a $PublishPacket with the given $topic, $payload, $qos and $retain.

  If $qos is 1, then allocates a packet id and returns it.
  */
  send_publish_ topic/string payload/ByteArray --qos/int --retain/bool -> int?:
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet_id := qos > 0 ? session_.next_packet_id : null

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet_id=packet_id

    send_ packet
    return packet_id

  /**
  Subscribes to the given $topic with a max qos of $max_qos.

  The $topic might have wildcards and is thus more of a filter than a single topic.

  Returns the packet id of the subscribe packet.
  */
  subscribe topic/string --max_qos/int=1 -> int:
    return subscribe_all [ TopicQos topic --max_qos=max_qos ]

  /**
  Subscribes to the given list $topics of type $TopicQos.

  Returns the packet id of the subscribe packet or -1 if the $topics is empty.
  */
  subscribe_all topics/List -> int:
    if topics.is_empty: return -1

    packet_id := session_.next_packet_id
    packet := SubscribePacket topics --packet_id=packet_id
    send_ packet
    return packet_id

  /**
  Unsubscribes from a single $topic.

  If the broker has a subscription (see $subscribe) of the given $topic, it will be removed.

  Returns the packet id of the unsubscribe packet.
  */
  unsubscribe topic/string -> int:
    return unsubscribe_all [topic]

  /**
  Unsubscribes from the list of $topics (of type $string).

  For each topic in the list of $topics, the broker checks if it has a subscription
    (see $subscribe) and removes it, if it exists.

  Returns the packet id of the unsubscribe packet or -1 if the $topics is empty.
  */
  unsubscribe_all topics/List -> int:
    if topics.is_empty: return -1

    packet_id := session_.next_packet_id
    packet := UnsubscribePacket topics --packet_id=packet_id
    send_ packet
    return packet_id

  /**
  Acknowledges the hand-over of the packet.

  If the packet has qos=1, sends an ack packet to the broker.

  If the client is closed, does nothing.
  */
  ack packet/Packet:
    if unacked_packet_ == packet: unacked_packet_ = null

    if is_closed: return
    if state_ != STATE_HANDLING_: throw "INVALID_STATE"

    if packet is PublishPacket:
      id := (packet as PublishPacket).packet_id
      if id:
        check_allowed_to_send_
        ack := PubAckPacket --packet_id=id
        // Skip the 'sending_' queue and write directly to the connection.
        // This way ack-packets are transmitted faster.
        do_connected_: connection_.write ack

  /**
  Sends the given $packet.

  Takes a lock and hands the packet to the connection.
  */
  send_ packet/Packet -> none:
    if packet is ConnectPacket: throw "INVALID_PACKET"
    check_allowed_to_send_
    sending_.do:
      // While waiting in the queue the client could have been closed.
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      do_connected_: connection_.write packet

  /**
  Runs the given $block in a connected state.

  If necessary reconnects the client.

  If $allow_disconnected is true, then the client may also be in a disconnected (but
    not closed) state.
  */
  do_connected_ --allow_disconnected/bool=false [block]:
    if is_closed and not (allow_disconnected and state_ == STATE_DISCONNECTED_): throw CLIENT_CLOSED_EXCEPTION
    while not Task.current.is_canceled:
      // If the connection is still alive, or if the manager doesn't want us to reconnect, let the
      // exception go through.
      should_abandon := :
        connection_.is_alive or reconnection_strategy_.is_closed or
          not reconnection_strategy_.should_try_reconnect transport_

      exception := catch --unwind=should_abandon:
        block.call
        return
      assert: exception != null
      if is_closed: throw CLIENT_CLOSED_EXCEPTION
      reconnect_ --is_initial_connection=false
      // While trying to reconnect there might have been a 'close' call.
      // Since we managed to connect, the 'close' call was too late to intervene with
      // the connection attempt. As such we consider the reconnection a success (as if
      // it happened before the 'close' call), and we continue.
      // There will be a 'disconnect' packet that will close the connection to the broker.

  refused_reason_for_return_code_ return_code/int -> string:
    refused_reason := "CONNECTION_REFUSED"
    if return_code == ConnAckPacket.UNACCEPTABLE_PROTOCOL_VERSION:
      refused_reason = "UNACCEPTABLE_PROTOCOL_VERSION"
    else if return_code == ConnAckPacket.IDENTIFIER_REJECTED:
      refused_reason = "IDENTIFIER_REJECTED"
    else if return_code == ConnAckPacket.SERVER_UNAVAILABLE:
      refused_reason = "SERVER_UNAVAILABLE"
    else if return_code == ConnAckPacket.BAD_USERNAME_OR_PASSWORD:
      refused_reason = "BAD_USERNAME_OR_PASSWORD"
    else if return_code == ConnAckPacket.NOT_AUTHORIZED:
      refused_reason = "NOT_AUTHORIZED"
    return refused_reason

  /**
  Connects (or reconnects) to the broker.

  Uses the reconnection_strategy to do the actual connecting.
  */
  reconnect_ --is_initial_connection/bool -> none:
    assert: not connection_ or not connection_.is_alive
    old_connection := connection_

    connecting_.do:
      // Check that nobody else reconnected while we waited to take the lock.
      if connection_ != old_connection: return

      if not connection_:
        assert: is_initial_connection
        connection_ = Connection_ transport_ --keep_alive=session_.options.keep_alive

      try:
        reconnection_strategy_.connect transport_
            --is_initial_connection = is_initial_connection
            --reconnect_transport = :
              transport_.reconnect
              connection_ = Connection_ transport_ --keep_alive=session_.options.keep_alive
            --send_connect = :
              packet := ConnectPacket session_.options.client_id
                  --clean_session=session_.options.clean_session
                  --username=session_.options.username
                  --password=session_.options.password
                  --keep_alive=session_.options.keep_alive
                  --last_will=session_.options.last_will
              connection_.write packet
            --receive_connect_ack = :
              response := connection_.read
              if not response: throw "CONNECTION_CLOSED"
              ack := (response as ConnAckPacket)
              return_code := ack.return_code
              if return_code != 0:
                refused_reason := refused_reason_for_return_code_ return_code
                connection_.close --reason=refused_reason
                // The broker refused the connection. This means that the
                // problem isn't due to a bad connection but almost certainly due to
                // some bad arguments (like a bad client-id). As such don't try to reconnect
                // again and just give up.
                close --force
                throw refused_reason
              ack.session_present
            --disconnect = :
              connection_.write DisconnectPacket
      finally: | is_exception exception |
        if is_exception:
          close --force

      // If we are here, then the reconnection succeeded.

      // Make sure the connection sends pings so the broker doesn't drop us.
      connection_.keep_alive --background

      // Resend the pending messages.
      session_.do --pending: | persisted/PersistedPacket |
        packet_id := persisted.packet_id
        topic := persisted.topic
        payload := persisted.payload
        retain := persisted.retain
        packet := PublishPacket topic payload --packet_id=packet_id --qos=1 --retain=retain --duplicate
        connection_.write packet

  /** Tears down the client. */
  tear_down_:
    critical_do:
      state_ = STATE_CLOSED_
      if reconnection_strategy_: reconnection_strategy_.close
      connection_.close
      if not handling_latch_.has_value: handling_latch_.set false

