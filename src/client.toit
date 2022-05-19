// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor
import log
import reader
import writer

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
  is_sending /bool := false
  sending_since /int? := null
  last_sent_us /int? := null

  constructor .wrapped_transport_:

  write bytes/ByteArray -> int:
    try:
      is_sending = true
      sending_since = Time.monotonic_us
      result := wrapped_transport_.write bytes
      last_sent_us = Time.monotonic_us
      return result
    finally:
      is_sending = false

  read -> ByteArray?:
    return wrapped_transport_.read

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

  keep_alive_ /Duration
  should_send_ping_ /bool := false
  activity_task_ /Task_? := null

  transport_ / ActivityMonitoringTransport_

  closing_reason_ /any := null

  writing_ /monitor.Mutex ::= monitor.Mutex

  constructor transport/Transport --keep_alive/Duration:
    keep_alive_ = keep_alive
    transport_ = ActivityMonitoringTransport_(transport)

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
    activity_task_ = task --background::
      catch:
        while is_alive:
          sleep_duration := check_activity_
          sleep sleep_duration
          activity_task_ = null

    try:
      catch --unwind=(: not is_closing and not is_closed):
        reader := reader.BufferedReader transport_
        while not is_closing:
          packet := Packet.deserialize reader
          block.call packet
    finally: | is_exception exception |
      close --reason=(is_exception ? exception : null)
      tear_down
      state_ = STATE_CLOSED_

  tear_down:
    if activity_task_:
      activity_task_.cancel
      activity_task_ = null

  close --reason=null:
    if is_closing or is_closed: return
    assert: closing_reason_ == null
    closing_reason_ = reason
    // By setting the state to closing we quell any error messages from disconnecting the transport.
    state_ = STATE_CLOSING_
    transport_.close

  /**
  Checks for activity.

  Returns a duration for when it wants to be called again.
  */
  check_activity_ -> Duration:
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
    else if not transport_.is_sending:
      // TODO(florian): we need to keep track of whether we have sent a ping.
      send PingReqPacket
      return keep_alive_ / 2
    else:
      // TODO(florian): we are currently sending.
      // We should detect timeouts on the sending.
      should_send_ping_ = true
      return keep_alive_

  send packet/Packet:
    // The writers should already be serialized by the Client, but we sometimes send
    // a ping packet from the handler. This packet is so small that it is unlikely that it
    // would be interleaved, but to be sure we have a lock here.
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
MQTT v3.1.1 Client with support for QoS 0 and 1.
*/
class ClientAdvanced:
  static DEFAULT_KEEP_ALIVE ::= Duration --s=60

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
  transport_ /Transport
  logger_ /log.Logger?

  connection_ /Connection_? := null
  connected_ /Barrier_ := Barrier_
  reconnect_done_ /monitor.Latch? := null

  subscriptions_ /Map := {:}

  next_packet_id_/int? := 1

  pending_ / Map ::= {:}  // int -> Packet

  closed_ /Barrier_ ::= Barrier_

  /**
  Serializes the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ / monitor.Mutex := monitor.Mutex

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $start to initiate the connection.
  */
  constructor --options/ClientOptions --transport/Transport --logger/log.Logger?:
    options_ = options
    transport_ = transport
    logger_ = logger

  start --on_packet/Lambda -> none:
    if state_ != STATE_CREATED_: throw "INVALID_STATE"
    assert: not connection_
    connect_
    handle_ --on_packet=on_packet

  handle_ --on_packet/Lambda -> none:
    try:
      while true:
        current_connection := connection_
        // TODO(florian): only catch if we can reconnect.
        exception := catch:
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
        if is_closed or is_closing: return
        reconnect_ --reason=exception --old_connection=current_connection
    finally:
      tear_down_
      connection_ = null

  reconnect_ --reason --old_connection/Connection_ -> none:
    print "reconnecting"
    if is_closing or is_closed: return

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
    reconnect_done_ = monitor.Latch
    try:
      transport_.reconnect
    finally: | is_exception exception |
      reconnect_done_.set (is_exception ? exception : null)
    connect_

  connect_:
    connection_ = Connection_ transport_ --keep_alive=options_.keep_alive
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
    state_ = STATE_CLOSED_
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

  send_ packet/Packet --packet_id/int? -> none:
    sending_.do:
      if packet is ConnectPacket: throw "INVALID_PACKET"
      while true:
        current_connection := connection_
        // TODO(florian): only catch if we can reconnect.
        exception := catch:
          check_connected_
          current_connection = connection_
          connection_.send packet
          if packet_id: pending_[packet_id] = packet
          return
        assert: exception != null
        reconnect_ --reason=exception --old_connection=current_connection

  ack packet/Packet:
    if packet is PacketIDAck:
      id := (packet as PacketIDAck).packet_id
      ack := PubAckPacket id
      connection_.send ack


class CallbackEntry_:
  callback /Lambda
  max_qos /int
  is_subscribed /bool := true

  constructor .callback .max_qos:

class SubscriptionTreeNode_:
  topic_level /string
  callback_entry_ /CallbackEntry_? := null
  children /Map ::= {:}  // string -> SubscriptionTreeNode_?

  constructor .topic_level:

/**
A tree of subscription, matching a topic to the registered callback.
*/
class SubscriptionTree_:
  root /SubscriptionTreeNode_ := SubscriptionTreeNode_ "ignored_root"

  /**
  Inserts, or replaces the callback for the given topic.

  Returns the old callback entry. Null if there was none.
  */
  add topic/string callback_entry/CallbackEntry_ -> CallbackEntry_?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_ := root
    topic_levels.do: | topic_level |
      node = node.children.get topic_level --init=: SubscriptionTreeNode_ topic_level
    result := node.callback_entry_
    node.callback_entry_ = callback_entry
    return result

  /**
  Removes the callback for the given topic.

  Returns the old callback entry. Null if there was none.
  */
  remove topic/string -> CallbackEntry_?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_? := root
    // Keep track of the parent node where we can (maybe) remove the child node from.
    // Any parent that has more than one child or has a callback must stay.
    parent_to_remove_from /SubscriptionTreeNode_? := root
    topic_level_to_remove /string? := null
    topic_levels.do: | topic_level |
      if node.callback_entry_ or node.children.size > 1:
        parent_to_remove_from = node
        topic_level_to_remove = topic_level

      node = node.children.get topic_level --if_absent=: throw "NOT SUBSCRIBED TO $topic"

    result := node.callback_entry_
    if node.children.is_empty:
      parent_to_remove_from.children.remove topic_level_to_remove
    else:
      node.callback_entry_ = null

    return result

  find topic/string -> CallbackEntry_?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_ := root
    catch_all_callback /CallbackEntry_? := null
    topic_levels.do: | topic_level |
      catch_all_node := node.children.get "#"
      if catch_all_node: catch_all_callback = catch_all_node.callback_entry_

      node = node.children.get topic_level
      if not node: node = node.children.get "+"
      if not node and not catch_all_callback: return null
      if not node: return catch_all_callback
    if node.callback_entry_: return node.callback_entry_
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

  start --attached/bool:
    if not attached: throw "INVALID_ARGUMENT"
    advanced_.start
        --on_packet = :: handle_packet_ it

  start --detached/bool --background/bool=false --on_error/Lambda=(:: logger_.error it) -> none:
    if not detached: throw "INVALID_ARGUMENT"
    task --background=background::
      exception := catch: start --attached
      if exception: on_error.call exception
    advanced_.when_connected: return

  handle_packet_ packet/Packet:
    // We ack the packet as soon as we call the registered callback.
    // This ensures that packets are acked in order (as required by the MQTT protocol).
    // It does not guarantee that the packet was correctly handled. If the callback
    // throws, the packet is not handled again.
    advanced_.ack packet

    if packet is PublishPacket:
      publish := packet as PublishPacket
      topic := publish.topic
      payload := publish.payload
      callback := subscription_callbacks_.find topic
      if callback:
        callback.callback.call topic payload
      else:
        // This can happen when the user unsubscribed from this topic but the
        // packet was already in the incoming queue.
        logger_.info "Received packet for unregistered topic $topic"
      return

    if packet is SubAckPacket:
      suback := packet as SubAckPacket
      if (suback.qos == 0x80):
        logger_.error "At least one subscription failed"

    // Ignore all other packets.

  is_closed -> bool:
    return advanced_.is_closed

  /**
  Publishes an MQTT message on $topic.

  The $qos parameter must be either:
  - 0: at most once, aka "fire and forget". In this configuration the message is sent, but the delivery
        is not guaranteed.
  - 1: at least once. The MQTT client ensures that the message is received by the MQTT broker.

  QoS = 2 (exactly once) is not implemented by this client.

  This method returns as soon as the message was written to the transport. It does *not* wait until
    the broker has returned with an acknowledgment.

  The $retain parameter lets the MQTT broker know whether it should retain this message. A new (later)
    subscription to this $topic would receive the retained message, instead of needing to wait for
    a new message on that topic.

  Not all MQTT brokers support $retain.
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false:
    advanced_.publish topic payload --qos=qos --retain=retain

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

    callback_entry := CallbackEntry_ callback max_qos
    old_entry := subscription_callbacks_.add filter callback_entry
    if old_entry:
      old_entry.is_subscribed = false
      if old_entry.max_qos == max_qos:
      // Just a simple change of callback.
      return

    advanced_.subscribe_all topic_filters

  /**
  Unsubscribes from a single topic $filter.

  The client must be connected to the $filter.
  */
  unsubscribe filter/string -> none:
    advanced_.unsubscribe filter

  close -> none:
    advanced_.close
