// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor
import log
import reader

import .transport
import .packets
import .topic_filter
import .tcp  // For toitdoc.

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
  last_sent_us /int? := null

  constructor .wrapped_transport_:

  send packet/Packet:
    wrapped_transport_.send packet
    last_sent_us = Time.monotonic_us

  receive --timeout/Duration? -> Packet:
    return wrapped_transport_.receive --timeout=timeout

  close -> none:
    wrapped_transport_.close

  supports_reconnect -> bool:
    return wrapped_transport_.supports_reconnect

  reconnect -> none:
    wrapped_transport_.reconnect


class ClientOptions:
  client_id  /string
  username   /string?
  password   /string?
  keep_alive /Duration?
  last_will  /LastWill?

  constructor
      .client_id
      --.username /string?
      --.password /string?
      --.keep_alive /Duration
      --.last_will /LastWill?:

interface Token:
  exception -> any

  wait_for_sent
  wait_for_ack

monitor Token_ implements Token:
  static STATE_CREATED_ ::= 0
  static STATE_QUEUED_ ::= 1 << 0
  static STATE_BEING_SENT_ ::= 1 << 1
  static STATE_SENT_ ::= 1 << 2
  static STATE_ACKED_ ::= 1 << 3
  static STATE_EXCEPTION_ ::= 1 << 4

  state_ := STATE_CREATED_

  packet / Packet? := ?
  exception_ := null

  constructor .packet:

  exception -> any:
    return exception_

  update_state state/int:
    state_ |= state

  set_exception exception:
    state_ |= STATE_EXCEPTION_
    exception_ = exception

  wait_for_ack:
    await: state_ & STATE_ACKED_ != 0 or state_ & STATE_EXCEPTION_ != 0

  wait_for_sent:
    await: state_ & STATE_SENT_ != 0 or state_ & STATE_EXCEPTION_ != 0

monitor WriterQueue_:
  // TODO(florian): we could prioritize Acks and pings.
  queue_ / Deque := Deque // of $Token.

  capacity /int

  constructor .capacity:

  add packet/Packet -> Token:
    await: size_ < capacity
    token := Token_ packet
    queue_.add token
    token.update_state Token_.STATE_QUEUED_
    return token

  size -> int:
    return size_

  get -> Token_:
    await: not empty_
    return queue_.remove_first

  size_ -> int:
    return queue_.size

  empty_ -> bool:
    return queue_.is_empty

  mark_closed:
    queue_.do: | token/Token_ |
      token.set_exception CLIENT_CLOSED_EXCEPTION


class Session_:
  /**
  Overhead room to give pings time to reach the server.
  The MQTT protocol requires activity from the client within a keep-alive duration.
  We send our ping a bit earlier to give communication time to reach the server. Servers
    generally give the clients some slack too, but there is no harm in being a bit eager on
    pings.
  */
  static KEEP_ALIVE_OVERHEAD_ROOM_US_ ::= 500_000

  /** The session has been created. No connection has been attempted yet. */
  static STATE_CREATED_ ::= 0
  /**
  The session is in the process of connecting.
  The connect package has not been sent yet.
  */
  static STATE_CONNECTING1_ ::= 1
  /**
  The session is in the process of connecting.
  The connect package has been sent, but no response has been received yet.
  */
  static STATE_CONNECTING2_ ::= 2
  /**
  The session is connected.
  Packets can be sent and received.
  */
  static STATE_CONNECTED_ ::= 3
  /**
  The session is in the process of disconnecting.
  Once connected, if there is an error during receiving or sending, the session will
    switch to this state and call $Transport.close. This will cause the
    other side (receive or send) to shut down as well (if there is any).
  Once the handler has finished cleaning up, the state switches to $STATE_CLOSED_.
  */
  static STATE_CLOSING_ ::= 4
  /**
  The session is shut down.
  */
  static STATE_CLOSED_ ::= 5

  options_   / ClientOptions
  transport_ / ActivityMonitoringTransport_
  logger_    / log.Logger?

  connected_   /Barrier_ ::= Barrier_

  closing_reason_ /any := null

  state_ / int := STATE_CREATED_

  connect_task_ /Task_? := null
  ping_task_ /Task_? := null
  writer_task_ /Task_? := null

  writer_queue_ /WriterQueue_ := ?

  constructor transport/Transport .options_ --logger/log.Logger? --outgoing_capacity/int:
    logger_ = logger
    transport_ = ActivityMonitoringTransport_(transport)
    writer_queue_ = WriterQueue_ outgoing_capacity

  is_connected -> bool: return state_ == STATE_CONNECTED_
  is_connecting -> bool:
    return state_ == STATE_CONNECTING1_ or state_ == STATE_CONNECTING2_
  is_closed -> bool: return state_ == STATE_CLOSED_
  is_closing -> bool: return state_ == STATE_CLOSING_

  /**
  Waits for the session to be connected and then calls the given $block.

  If the session could not connect throws.
  If the session is closed throws.
  */
  when_connected [block]:
    check_connected_
    block.call

  check_connected_:
    exception := connected_.get
    if exception: throw exception
    // Check that we are still connected and haven't been closed in the meantime.
    if not is_connected: throw CLIENT_CLOSED_EXCEPTION

  /**
  Connects to the server and handles incoming packets.

  Only returns when the session is closed.
  Returns null if the session is cleanly closed.
  Returns the reason for the closing, otherwise.
  */
  handle [block]:
    try:
      exception := catch --trace=(: should_trace_exception_ it):
        while not is_closing:
          packet := transport_.receive --timeout=null

          if packet is ConnAckPacket:
            handle_connack_ (packet as ConnAckPacket)
          else if packet is PingRespPacket:
            // Ignore.
          else:
            block.call packet

      close --reason=exception
    finally:
      tear_down_
    assert: is_closing
    state_ = STATE_CLOSED_
    return closing_reason_

  /**
  Tears down the connection/session.

  This function is called both for graceful and ungraceful shutdowns.
  It ensures that allocated resources are freed and waiting clients can resume.
  */
  tear_down_:
    // We need to be able to close even when canceled, so we run the
    // close steps in a critical region.
    // TODO(florian): is this the only place? Do we really need this?
    critical_do:
      if connect_task_:
        connect_task_.cancel
        connect_task_ = null
      if ping_task_:
        ping_task_.cancel
        ping_task_ = null
      if writer_task_:
        writer_task_.cancel
        writer_task_ = null
      writer_queue_.mark_closed
      if not connected_.has_value:
        connected_.set CLIENT_CLOSED_EXCEPTION

  close --reason=null:
    if is_closing or is_closed: return
    assert: closing_reason_ == null
    closing_reason_ = reason
    // By setting the state to closing we quell any error messages from disconnecting the transport.
    // See $should_trace_exception_.
    state_ = STATE_CLOSING_
    transport_.close

  should_trace_exception_ exception:
    // We expect to see exceptions when we shut down the transport.
    // Normally these should be in the closing phase, however, the handler might shut down
    //   quite fast, in which case the session might already be fully closed.
    return not (is_closing or is_closed)

  connect:
    connect_task_ = task --background::
      connect_
      connect_task_ = null
    connected_.get

  send packet/Packet -> Token:
    if packet is ConnectPacket: throw "INVALID_PACKET"
    check_connected_
    return writer_queue_.add packet

  connect_:
      state_ = STATE_CONNECTING1_
      connect := ConnectPacket options_.client_id
          --username=options_.username
          --password=options_.password
          --keep_alive=options_.keep_alive
          --last_will=options_.last_will
      // No need to handle the exception. The 'write_' propagates the exception to the
      // handler task.
      catch:
        write_ connect
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
    ping_task_ = task --background:: activity_checker_
    writer_task_ = task --background:: handle_outgoing_
    connected_.set null

  activity_checker_:
    // TODO(florian): we should be more clever here:
    // We should monitor when the transport starts writing, and when it gets a chunk through.
    // Also, we should monitor that we actually get something from the server.
    while is_connected:
      remaining_keep_alive_us := options_.keep_alive.in_us - (Time.monotonic_us - transport_.last_sent_us)
      // Decrease it to give some room for overhead.
      remaining_keep_alive_us -= KEEP_ALIVE_OVERHEAD_ROOM_US_
      if remaining_keep_alive_us > 0:
        remaining_keep_alive := Duration --us=remaining_keep_alive_us
        sleep remaining_keep_alive
      else:
        // No need to handle the exception. The 'write_' propagates the exception to the
        // handler task.
        exception := catch: write_ PingReqPacket
        if exception: break
        sleep (options_.keep_alive / 2)
    ping_task_ = null

  handle_outgoing_:
    while is_connected:
      token /Token_ := writer_queue_.get
      token.update_state Token_.STATE_BEING_SENT_
      // No need to handle the exception. The 'write_' propagates the exception to the
      // handler task.
      exception := catch:
        write_ token.packet
        token.update_state Token_.STATE_SENT_
      if exception: break


  write_ packet/Packet:
    exception := catch --trace=(: should_trace_exception_ it):
      transport_.send packet
    if exception:
      if is_closing or is_closed:
        throw CLIENT_CLOSED_EXCEPTION
      close --reason=exception
      throw exception


/**
MQTT v3.1.1 Client with support for QoS 0 and 1.
*/
class ClientAdvanced:
  static DEFAULT_KEEP_ALIVE ::= Duration --s=60
  static DEFAULT_OUTGOING_CAPACITY ::= 8

  /** The client has been created. Handle has not been called yet. */
  static STATE_CREATED_ ::= 0
  /** The client is connecting. */
  static STATE_CONNECTING_ ::= 1
  /** The client is connected. */
  static STATE_CONNECTED_ ::= 2
  /** The client is disconnected. */
  static STATE_DISCONNECTED_ ::= 3
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
  transport_ /Transport
  logger_ /log.Logger?

  session_ /Session_? := null
  subscriptions_ /Map := {:}

  next_packet_id_/int? := 1

  pending_ / Map ::= {:}  // int -> Token
  closed_ /Barrier_ ::= Barrier_

  handle_task_ /Task_? := null

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $start to initiate the connection.
  */
  constructor --options/ClientOptions --transport/Transport --logger/log.Logger?:
    options_ = options
    transport_ = transport
    logger_ = logger

  start -> none
      --background/bool=false
      --on_error/Lambda
      --on_packet/Lambda:
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not session_
    session_ = Session_ transport_ options_ --logger=logger_ --outgoing_capacity=DEFAULT_OUTGOING_CAPACITY
    state_ = STATE_CONNECTING_
    handle_task_ = task --background=background::
      exception := handle_ --on_packet=on_packet
      if exception: on_error.call exception
    // Just catch the call to connect. If there is an error, then the `on_error` function
    // reports it.
    catch: session_.connect_

  handle_ --on_packet/Lambda -> any:
    try:
      exception := session_.handle: | packet/Packet |
        if packet is PublishPacket:
          publish := packet as PublishPacket
          topic := publish.topic
          payload := publish.payload
          on_packet.call packet
          if publish.packet_id:
            ack := PubAckPacket publish.packet_id
            session_.send ack
        else if packet is PacketIDAck:
          ack := packet as PacketIDAck
          id := ack.packet_id
          pending_.get id
              --if_present=: | token / Token_ |
                token.update_state Token_.STATE_ACKED_
                pending_.remove id
              --if_absent=: logger_.info "unmatched packet id: $id"
      return exception
    finally:
      tear_down_
      session_ = null

  /**
  Tears down the client.
  */
  tear_down_:
    pending_.do --values: it.set null
    state_ = STATE_CLOSED_
    closed_.set true

  /**
  Closes the MQTT client.

  Unless the client is already closed, executes an orderly disconnect.
  */
  close:
    if is_closing or is_closed: return

    state_ = STATE_CLOSING_

    // Note that disconnect packets don't need a packet id (which is important as
    // the packet_id counter is used as marker that the client is closed).
    if session_.is_connected:
      catch --trace: session_.send DisconnectPacket

    // The session disconnect will stop the $Session_.handle. This in turn will invoke
    // the `tear_down` in $start method.
    session_.close

    closed_.get

  /**
  Whether the client is closed.
  */
  is_closed -> bool:
    return state_ == STATE_CLOSED_

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
  publish topic/string payload/ByteArray --qos=1 --retain=false -> Token:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet_id := qos > 0 ? next_packet_id_++ : null

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet_id=packet_id

    return send_ packet --packet_id=(qos > 0 ? packet_id : null)

  /**
  Subscribes to the given list $topic_filters of type $TopicFilter.

  All messages that are received this way are delivered to the `on_packet` lambda
    given to $start.
  */
  subscribe_all topic_filters/List -> Token:
    if topic_filters.is_empty: throw "INVALID_ARGUMENT"
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    // TODO(florian): can we match topics to callbacks?
    packet_id := next_packet_id_++
    packet := SubscribePacket topic_filters --packet_id=packet_id
    return send_ packet --packet_id=packet_id

  /**
  Unsubscribes from a single topic $filter.
  */
  unsubscribe filter/string -> none:
    // Not implemented yet.

  send_ packet/Packet --packet_id/int? -> Token:
    token := session_.send packet
    if packet_id: pending_[packet_id] = token
    return token

  static should_trace_exception_ exception/any -> bool:
    if exception == "NOT_CONNECTED": return false
    if exception == reader.UNEXPECTED_END_OF_READER_EXCEPTION: return false
    return true

class SubscriptionTreeNode_:
  topic_level /string
  callback /Lambda? := null
  max_qos /int? := null
  children /Map ::= {:}  // string -> SubscriptionTreeNode_?

  constructor .topic_level:

/**
A tree of subscription, matching a topic to the registered callback.
*/
class SubscriptionTree_:
  root /SubscriptionTreeNode_ := SubscriptionTreeNode_ "ignored_root"

  /**
  Inserts, or replaces the callback for the given topic.

  Returns the old qos. Null if there was no callback.
  */
  add topic/string callback/Lambda --max_qos/int -> int?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_ := root
    topic_levels.do: | topic_level |
      node = node.children.get topic_level --init=: SubscriptionTreeNode_ topic_level
    result := node.max_qos
    node.callback = callback
    node.max_qos = max_qos
    return result

  remove topic/string -> none:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_? := root
    // Keep track of the parent node where we can (maybe) remove the child node from.
    // Any parent that has more than one child or has a callback must stay.
    parent_to_remove_from /SubscriptionTreeNode_? := root
    topic_level_to_remove /string? := null
    topic_levels.do: | topic_level |
      if node.callback or node.children.size > 1:
        parent_to_remove_from = node
        topic_level_to_remove = topic_level

      node = node.children.get topic_level --if_absent=: throw "NOT SUBSCRIBED TO $topic"

    if node.children.is_empty:
      parent_to_remove_from.children.remove topic_level_to_remove
    else:
      node.callback = null
      node.max_qos = null

  find topic/string -> Lambda:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_ := root
    catch_all_callback /Lambda? := null
    topic_levels.do: | topic_level |
      catch_all_node := node.children.get "#"
      if catch_all_node: catch_all_callback = catch_all_node.callback

      node = node.children.get topic_level
      if not node: node = node.children.get "+"
      if not node and not catch_all_callback: throw "NOT SUBSCRIBED TO $topic"
      if not node: return catch_all_callback
    if node.callback: return node.callback
    return catch_all_callback

class Client:
  advanced_ /ClientAdvanced

  subscription_callbacks_ /SubscriptionTree_ := SubscriptionTree_
  logger_ /log.Logger?

  /**
  Constructs a new MQTT client.

  The $client_id (client identifier) will be used by the broker to identify a client.
    It should be unique per broker and can be between 1 and 23 characters long.
    Only characters and numbers are allowed

  The $transport parameter is used to send messages and is usually a TCP socket instance.
    See $TcpTransport.

  If necessary, the $username/$password credentials can be used to authenticate.

  The $keep_alive informs the server of the maximum duration between two packets.
    The client automatically sends PINGREQ messages when necessary. If the value is
    lower, then the server detects disconnects faster, but the client needs to send
    more messages.

  When provided, the $last_will configuration is used to send when the client
    disconnects ungracefully.
  */
  constructor
      --client_id /string
      --transport /Transport
      --logger /log.Logger? = log.default
      --username /string? = null
      --password /string? = null
      --keep_alive /Duration = ClientAdvanced.DEFAULT_KEEP_ALIVE
      --last_will /LastWill? = null:
    logger_ = logger
    options := ClientOptions client_id
        --username=username
        --password=password
        --keep_alive=keep_alive
        --last_will=last_will
    advanced_ = ClientAdvanced --options=options --transport=transport --logger=logger

  start --on_error/Lambda=(:: logger_.error it) -> none:
    advanced_.start
        --on_error = on_error
        --on_packet = :: | packet |
          exception := catch --trace:
            topic := packet.topic
            payload := packet.payload
            callback := subscription_callbacks_.find topic
            callback.call packet.topic packet.payload
          if exception: on_error.call exception

  is_closed -> bool:
    return advanced_.is_closed

  /**
  Publishes an MQTT message on $topic.

  The $qos parameter must be either:
  - 0: at most once, aka "fire and forget". In this configuration the message is sent, but the delivery
        is not guaranteed.
  - 1: at least once. The MQTT client ensures that the message is received by the MQTT broker.

  QoS = 2 (exactly once) is not implemented by this client.

  The $retain parameter lets the MQTT broker know whether it should retain this message. A new (later)
    subscription to this $topic would receive the retained message, instead of needing to wait for
    a new message on that topic.

  Not all MQTT brokers support $retain.
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false:
    token := advanced_.publish topic payload --qos=qos --retain=retain

    if qos == 0:
      token.wait_for_sent
      if token.exception: throw token.exception
    else:
      token.wait_for_ack
      if token.exception: throw token.exception

  /**
  Subscribes to a single topic $filter, with the provided $max_qos.

  The chosen $max_qos is the maximum QoS the client will receive. The broker
    generally sends a packet to subscribers with the same QoS as the one it
    received it with. The $max_qos parameter sets a limit on which QoS the client
    wants to receive.

  See $publish for an explanation of the different QoS values.
  */
  subscribe filter/string --max_qos/int=1 callback/Lambda:
    topic_filters := [ TopicFilter filter --max_qos=max_qos ]
    if topic_filters.is_empty: throw "INVALID_ARGUMENT"

    old_qos := subscription_callbacks_.add filter callback --max_qos=max_qos
    if old_qos == max_qos:
      // Just a simple change of callback.
      return
    token := advanced_.subscribe_all topic_filters
    token.wait_for_ack

  /**
  Unsubscribes from a single topic $filter.

  The client must be connected to the $filter.
  */
  unsubscribe filter/string -> none:
    advanced_.unsubscribe filter

  close -> none:
    advanced_.close
