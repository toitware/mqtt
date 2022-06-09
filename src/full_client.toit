// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor
import log
import reader
import writer

import .session_options
import .last_will
import .packets
import .tcp  // For toitdoc.
import .topic_qos
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
    if keep_alive_duration_ == (Duration --s=0): return

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
A base class for reconnection strategies.
*/
abstract class DefaultReconnectionStrategyBase implements ReconnectionStrategy:
  UNIMPLEMENTED ::= "UNIMPLEMENTED"

/**
The default reconnection strategy for clients that are connected with the
  clean session bit.

Since the broker will drop the session information this strategy won't reconnect
  when the connection breaks after it has connected. However, it will potentially
  try to connect multiple times for the initial connection.
*/
class DefaultCleanSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:
  UNIMPLEMENTED ::= "UNIMPLEMENTED"

/**
The default reconnection strategy for clients that are connected without the
  clean session bit.

If the connection drops, the client tries to reconnect potentially multiple times.
*/
class DefaultSessionReconnectionStrategy extends DefaultReconnectionStrategyBase:
  UNIMPLEMENTED ::= "UNIMPLEMENTED"

/**
An MQTT session.

Keeps track of data that must survive disconnects (assuming the user didn't use
  the 'clean_session' flag).
*/
class Session_:
  next_packet_id_ /int? := 1

  /** Packets that are still missing an ack. */
  pending_ /Map ::= {:}  // From packet_id to persistent_id.

  options /SessionOptions

  constructor .options:

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
  /**
  Stores the publish-packet information in the persistent store.
  Returns a persistent-store id which can be used to $get or $remove the data
    from the store.
  */
  store topic/string payload/ByteArray --retain/bool -> int

  /**
  Finds the persistent packet with $persistent_id and calls the given $block
    with arguments topic, payload and retain (in that order).

  The store may decide not to resend a packet, in which case it calls
    $if_absent with the $persistent_id.
  */
  get persistent_id/int [block] [--if_absent] -> none

  /**
  Removes the data for the packet with $persistent_id.
  Does nothing if there is no data associated to $persistent_id.
  */
  remove persistent_id/int -> none

  /**
  Calls the given block for each stored packet.

  The arguments to the block are:
  - the persistent id
  - the topic
  - the payload
  - the retain flag
  */
  do [block] -> none

/**
A persistence store that stores the packets in memory.
*/
class MemoryPersistenceStore implements PersistenceStore:
  UNIMPLEMENTED ::= "UNIMPLEMENTED"

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
  handling_latch_ /monitor.Latch := monitor.Latch

  /**
  A mutex to queue the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex

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
      --logger /log.Logger = log.default
      --persistence_store /PersistenceStore? = null:
    transport_ = ActivityMonitoringTransport.private_(transport)
    logger_ = logger
    this.persistence_store = persistence_store or MemoryPersistenceStore

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
      --reconnection_strategy /ReconnectionStrategy? = null:
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_

    if reconnection_strategy:
      reconnection_strategy_ = reconnection_strategy
    else if options.clean_session:
      reconnection_strategy_ = DefaultCleanSessionReconnectionStrategy
    else:
      reconnection_strategy_ = DefaultSessionReconnectionStrategy

    session_ = Session_ options

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
      while true:
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
            throw reader.UNEXPECTED_END_OF_READER_EXCEPTION
          // The user called 'close'. Doesn't mean it was always completely graceful, but
          // good enough.
          break

        if packet is PublishPacket or packet is SubAckPacket or packet is UnsubAckPacket:
          unacked_packet_ = packet
          try:
            on_packet.call packet
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
          persistence_id := session_.handle_ack id
              --if_absent=: logger_.info "unmatched packet id: $id"
          if persistence_id: persistence_store.remove persistence_id
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

    // There might be an attempt of reconnecting in progress.
    // By closing here, we make that attempt shorter (if it fails).
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
      persistent_id := persistence_store.store topic payload --retain=retain
      session_.set_pending_ack --packet_id=packet_id --persistent_id=persistent_id

  /**
  Publishes the MQTT message stored in the persistence store identified by $persistent_id.
  */
  publish_persisted persistent_id/int --qos=1 -> none:
    persistence_store.get persistent_id
        --if_absent=: throw "PERSISTED_MESSAGE_NOT_FOUND"
        : | topic payload retain |
          packet_id := send_publish_ topic payload --qos=qos --retain=retain
          if qos == 1:
            session_.set_pending_ack --packet_id=packet_id --persistent_id=persistent_id

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

    // Reconnection is implemented in follow-up PR.
    block.call

  /**
  Connects (or reconnects) to the broker.

  Uses the reconnection_strategy to do the actual connecting.
  */
  reconnect_ --is_initial_connection/bool -> none:
    throw "UNIMPLEMENTED"  // follow-up PR.

  /** Tears down the client. */
  tear_down_:
    critical_do:
      state_ = STATE_CLOSED_
      if reconnection_strategy_: reconnection_strategy_.close
      connection_.close
      if not handling_latch_.has_value: handling_latch_.set false

