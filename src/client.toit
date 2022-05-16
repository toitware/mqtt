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

  disconnect -> none:
    wrapped_transport_.disconnect

  supports_reconnect -> bool:
    return wrapped_transport_.supports_reconnect

  reconnect -> none:
    wrapped_transport_.reconnect


class ClientOptions_:
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
    switch to this state and call $Transport.disconnect. This will cause the
    other side (receive or send) to shut down as well (if there is any).
  Once the handler has finished cleaning up, the state switches to $STATE_DISCONNECTED_.
  */
  static STATE_DISCONNECTING_ ::= 4
  /**
  The session is shut down.
  */
  static STATE_DISCONNECTED_ ::= 5

  options_   / ClientOptions_
  transport_ / ActivityMonitoringTransport_
  logger_    / log.Logger

  connected_   /Barrier_ ::= Barrier_

  disconnect_reason_ /any := null

  state_ / int := STATE_CREATED_

  handler_task /Task_? := null
  connect_task_ /Task_? := null
  ping_task_ /Task_? := null

  writing_ /monitor.Mutex := monitor.Mutex

  constructor transport/Transport .options_ --logger/log.Logger:
    logger_ = logger
    transport_ = ActivityMonitoringTransport_(transport)

  is_connected -> bool: return state_ == STATE_CONNECTED_
  is_connecting -> bool:
    return state_ == STATE_CONNECTING1_ or state_ == STATE_CONNECTING2_
  is_disconnected -> bool: return state_ == STATE_DISCONNECTED_
  is_disconnecting -> bool: return state_ == STATE_DISCONNECTING_

  /**
  Waits for the session to be connected and then calls the given $block.

  If the session could not connect throws.
  If the session is disconnected throws.
  */
  when_connected [block]:
    check_connected_
    block.call

  check_connected_:
    exception := connected_.get
    if exception: throw exception
    // Check that we are still connected and haven't been disconnected in the meantime.
    if not is_connected: throw CLIENT_CLOSED_EXCEPTION

  /**
  Connects to the server and handles incoming packets.

  Only returns when the session is disconnected.
  Returns null if the session is cleanly disconnected.
  Returns the reason for the disconnect, otherwise.
  */
  handle [block]:
    handler_task = task
    try:
      exception := catch --trace=(: should_trace_exception_ it):
        while not is_disconnecting:
          packet := transport_.receive --timeout=null

          if packet is ConnAckPacket:
            handle_connack_ (packet as ConnAckPacket)
          else if packet is PingRespPacket:
            // Ignore.
          else:
            block.call packet

      disconnect --reason=exception
    finally:
      tear_down_
    assert: is_disconnecting
    state_ = STATE_DISCONNECTED_
    return disconnect_reason_

  /**
  Tears down the connection/session.

  This function is called both for graceful and ungraceful shutdowns.
  It ensures that allocated resources are freed and waiting clients can resume.
  */
  tear_down_:
    assert: task == handler_task
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
      if not connected_.has_value:
        connected_.set CLIENT_CLOSED_EXCEPTION

  disconnect --reason=null:
    if is_disconnecting or is_disconnected: return
    assert: disconnect_reason_ == null
    disconnect_reason_ = reason
    // By setting the state to disconnecting we quell any error messages from disconnecting the transport.
    // See $should_trace_exception_.
    state_ = STATE_DISCONNECTING_
    transport_.disconnect

  should_trace_exception_ exception:
    // We expect to see exceptions when we shut down the transport.
    // Normally these should be in the disconnecting phase, however, the handler might shut down
    //   quite fast, in which case the session might already be fully disconnected.
    return not (is_disconnecting or is_disconnected)

  send packet/Packet:
    if packet is ConnectPacket:
      if state_!= STATE_CONNECTING1_: throw "INVALID_STATE"
    else:
      check_connected_
    // There can be different writers at once. (The ping task, for example).
    // Make sure we don't interleave packets on the wire.
    writing_.do:
      exception := catch --trace=(: should_trace_exception_ it):
        transport_.send packet
      if exception:
        if is_disconnecting or is_disconnected:
          throw CLIENT_CLOSED_EXCEPTION
        disconnect --reason=exception
        throw exception

  connect:
    connect_task_ = task:: connect_
    check_connected_

  connect_:
    assert: task == connect_task_
    state_ = STATE_CONNECTING1_
    connect := ConnectPacket options_.client_id
        --username=options_.username
        --password=options_.password
        --keep_alive=options_.keep_alive
        --last_will=options_.last_will
    // No need to handle the exception. The 'send' propagates the exception to the
    // handler task.
    catch:
      send connect
      state_ = STATE_CONNECTING2_
    connect_task_ = null

  handle_connack_ packet/ConnAckPacket:
    if state_ != STATE_CONNECTING2_:
      logger_.info "Received spurious CONNACK"
      return

    if packet.return_code != 0:
      state_ = STATE_DISCONNECTED_
      connected_.set "connection refused: $packet.return_code"
      return

    state_ = STATE_CONNECTED_
    ping_task_ = task:: activity_checker_
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
        // No need to handle the exception. The 'send' propagates the exception to the
        // handler task.
        exception := catch: send PingReqPacket
        if exception: break
        sleep (options_.keep_alive / 2)
    ping_task_ = null


/**
MQTT v3.1.1 Client with support for QoS 0 and 1.
*/
class Client:
  static DEFAULT_KEEP_ALIVE ::= Duration --s=60

  /** The client has been created. Handle has not been called yet. */
  static STATE_CREATED_ ::= 0
  /** The client is starting up, as the handle function has been called. */
  static STATE_HANDLING_ ::= 1
  /** The client is connecting. */
  static STATE_CONNECTING_ ::= 2
  /** The client is connected. */
  static STATE_CONNECTED_ ::= 3
  /** The client is disconnected. */
  static STATE_DISCONNECTED_ ::= 4
  /**
  The client is disconnected and in the process of shutting down.
  This only happens once the current message has been handled. That is, once
    the $handle method's block has returned.
  */
  static STATE_CLOSING_ ::= 5
  /** The client is closed. */
  static STATE_CLOSED_ ::= 6

  state_ /int := STATE_CREATED_

  options_ /ClientOptions_
  transport_ /Transport
  logger_ /log.Logger

  session_ /Session_? := null
  subscriptions_ /Set := {}

  next_packet_id_/int? := 1

  sending_/monitor.Mutex ::= monitor.Mutex
  pending_/Map/*<int, monitor.Latch>*/ ::= {:}
  closed_ /Barrier_ ::= Barrier_

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $handle to initiate the connection.

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
      client_id /string
      transport /Transport
      --logger /log.Logger = log.default
      --username /string? = null
      --password /string? = null
      --keep_alive /Duration = DEFAULT_KEEP_ALIVE
      --last_will /LastWill? = null:
    transport_ = transport
    logger_ = logger
    options_ = ClientOptions_ client_id
        --username=username
        --password=password
        --keep_alive=keep_alive
        --last_will=last_will

  handle [block]:
    if session_: throw "ALREADY_RUNNING"
    session_ = Session_ transport_ options_ --logger=logger_
    try:
      exception := session_.handle: | packet/Packet |
        if packet is PublishPacket:
          publish := packet as PublishPacket
          block.call publish.topic publish.payload
          if publish.packet_id:
            ack := PubAckPacket publish.packet_id
            session_.send ack
        else if packet is PacketIDAck:
          ack := packet as PacketIDAck
          pending_.get ack.packet_id
              --if_present=: it.set ack
              --if_absent=: logger_.info "unmatched packet id: $ack.packet_id"
      if exception: throw exception
    finally:
      tear_down_
      session_ = null

  /**
  Tears down the client.

  This function is called both for graceful and ungraceful shutdowns.
  It ensures that allocated resources are freed and waiting clients can resume.
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
    // the `tear_down` in $run method.
    session_.disconnect

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
  publish topic/string payload/ByteArray --qos=1 --retain=false:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet_id := qos > 0 ? next_packet_id_++ : null

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet_id=packet_id

    // If we don't have a packet identifier (QoS == 0), don't wait for an ack.
    if not packet_id:
      session_.send packet
      return

    wait_for_ack_ packet_id: | latch/monitor.Latch |
      session_.send packet
      ack := latch.get
      if not ack: throw CLIENT_CLOSED_EXCEPTION

  /**
  Subscribe to a single topic $filter, with the provided $qos.

  See $publish for an explanation of the different QOS values.
  */
  subscribe filter/string --qos/int=1:
    subscribe_all [TopicFilter filter --qos=qos]

  /**
  Subscribe to tha list a $topic_filters of type $TopicFilter.

  Each topic filter has its own QoS, that the server will verify
    before returning.
  */
  subscribe_all topic_filters/List:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    packet_id := next_packet_id_++
    packet := SubscribePacket topic_filters --packet_id=packet_id
    wait_for_ack_ packet_id: | latch/monitor.Latch |
      session_.send packet
      ack := latch.get
      if not ack: throw CLIENT_CLOSED_EXCEPTION

  /**
  Unsubscribe from a single topic $filter.
  */
  unsubscribe filter/string -> none:
    // Not implemented yet.

  wait_for_ack_ packet_id [block]:
    latch := monitor.Latch
    pending_[packet_id] = latch
    try:
      block.call latch
    finally:
      pending_.remove packet_id


  static should_trace_exception_ exception/any -> bool:
    if exception == "NOT_CONNECTED": return false
    if exception == reader.UNEXPECTED_END_OF_READER_EXCEPTION: return false
    return true
