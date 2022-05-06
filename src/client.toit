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
MQTT v3.1.1 Client with support for QoS 0 and 1.

All received messages are processed by a single call to $handle:

  client := mqtt.Client ...
  task::
    client.handle: | topic/string payload/ByteArray |
      print "Received message on topic '$topic': $payload"

Calls to $subscribe can be done at any time, with new messages arriving
  at the existing call to $handle.

If the client is closed, $handle will gracefully return. Any other ongoing
  calls will throw an exception.
*/
class Client:
  static DEFAULT_KEEP_ALIVE ::= Duration --s=60

  transport_/Transport
  logger_/log.Logger
  keep_alive_/Duration?

  next_packet_id_/int? := 1  // Field is `null` when client is closed.
  last_sent_us_/int := ?
  task_ := null

  connected_/monitor.Latch ::= monitor.Latch
  sending_/monitor.Mutex ::= monitor.Mutex
  incoming_/monitor.Channel ::= monitor.Channel 8
  pending_/Map/*<int, monitor.Latch>*/ ::= {:}

  /**
  Constructs an MQTT client.

  The $client_id (client identifier) will be used by the broker to identify a client.
    It should be unique per broker and can be between 1 and 23 characters long.
    Only characters and numbers are allowed

  The $transport_ parameter is used to send messages and is usually a TCP socket instance.
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
      client_id/string
      .transport_
      --logger=log.default
      --username/string?=null
      --password/string?=null
      --keep_alive/Duration=DEFAULT_KEEP_ALIVE
      --last_will/LastWill?=null:
    keep_alive_ = keep_alive
    logger_ = logger
    // Initialize with the current time.
    // We are doing a connection request just below.
    last_sent_us_ = Time.monotonic_us

    task_ = task --background::
      try:
        catch --trace=(: should_trace_exception_ it):
          run_
      finally:
        task_ = null
        close

    connect := ConnectPacket client_id
        --username=username
        --password=password
        --keep_alive=keep_alive
        --last_will=last_will
    send_ connect
    ack/ConnAckPacket := connected_.get
    if ack.return_code != 0:
      close
      throw "connection refused: $ack.return_code"

  /**
  Closes the MQTT client.
  */
  close:
    if is_closed: return
    // Mark as closed.
    next_packet_id_ = null
    // We need to be able to close even when canceled, so we run the
    // close steps in a critical region.
    critical_do:
      // TODO(anders): The incoming buffer can be full in which case this will
      // block. All we want to achieve is to unblock the corresponding call to
      // receive, so the task stuck in $handle can continue.
      incoming_.send null
      catch --trace=(: should_trace_exception_ it):
        send_ DisconnectPacket
      pending_.do --values: it.set null
      if task_: task_.cancel

  /**
  Whether the client is closed.
  */
  is_closed -> bool:
    return next_packet_id_ == null

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
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    packet_id := qos > 0 ? next_packet_id_++ : null

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet_id=packet_id

    // If we don't have a packet identifier (QoS == 0), don't wait for an ack.
    if not packet_id:
      send_ packet
      return

    wait_for_ack_ packet_id: | latch/monitor.Latch |
      send_ packet
      ack := latch.get
      if not ack: throw CLIENT_CLOSED_EXCEPTION

  /**
  Subscribe to a single topic $filter, with the provided $qos.

  See $publish for an explanation of the different QOS values.
  */
  subscribe filter/string --qos/int:
    subscribe [TopicFilter filter --qos=qos]

  /**
  Subscribe to a list a $topic_filters.

  Each topic filter has its own QoS, that the server will verify
    before returning.
  */
  subscribe topic_filters/List:
    if is_closed: throw CLIENT_CLOSED_EXCEPTION
    packet_id := next_packet_id_++
    packet := SubscribePacket topic_filters --packet_id=packet_id
    wait_for_ack_ packet_id: | latch/monitor.Latch |
      send_ packet
      ack := latch.get
      if not ack: throw CLIENT_CLOSED_EXCEPTION

  /**
  Unsubscribe to a single topic $filter.
  */
  unsubscribe filter/string -> none:
    // Not implemented yet.

  /**
  Handle incoming messages. The $block is called with two arguments,
    the topic (a string) and the payload (a ByteArray).
  */
  handle [block]:
    while true:
      publish/PublishPacket? := incoming_.receive
      if not publish: return
      block.call publish.topic publish.payload
      if publish.packet_id:
        ack := PubAckPacket publish.packet_id
        send_ ack

  send_ packet/Packet:
    // Any number of different tasks can start sending packets. It
    // is critical that the packet bits sent over the transport stream
    // aren't interleaved, so we use a mutex to serialize the sends.
    sending_.do:
      exception := catch --trace=(: should_trace_exception_ it):
        transport_.send packet
      if exception:
        if is_closed: return
        if transport_ is ReconnectingTransport:
          (transport_ as ReconnectingTransport).reconnect
          // Try again.
          transport_.send packet
      last_sent_us_ = Time.monotonic_us

  wait_for_ack_ packet_id [block]:
    latch := monitor.Latch
    pending_[packet_id] = latch
    try:
      block.call latch
    finally:
      pending_.remove packet_id

  run_:
    while not task.is_canceled:
      remaining_keep_alive_us := keep_alive_.in_us - (Time.monotonic_us - last_sent_us_)
      packet := null
      if remaining_keep_alive_us > 0:
        remaining_keep_alive := Duration --us=remaining_keep_alive_us
        // Timeout returns a `null` packet.
        exception := catch --trace=(: should_trace_exception_ it):
          packet = transport_.receive --timeout=remaining_keep_alive
        if exception:
          if is_closed: return
          if transport_ is ReconnectingTransport:
            (transport_ as ReconnectingTransport).reconnect
            continue

      if packet == null:
        ping := PingReqPacket
        send_ ping
      else if packet is ConnAckPacket:
        connected_.set packet
      else if packet is PublishPacket:
        publish := packet as PublishPacket
        incoming_.send publish
      else if packet is PacketIDAck:
        ack := packet as PacketIDAck
        pending_.get ack.packet_id
            --if_present=: it.set ack
            --if_absent=: logger_.info "unmatched packet id: $ack.packet_id"
      else if packet is PingRespPacket:
        // Do nothing.
      else:
        throw "unhandled packet type: $packet.type"

  static should_trace_exception_ exception/any -> bool:
    if exception == "NOT_CONNECTED": return false
    if exception == reader.UNEXPECTED_END_OF_READER_EXCEPTION: return false
    return true
