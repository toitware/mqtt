// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor
import log

import .transport
import .packets
import .topic_filter

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

  task_ := null
  next_packet_id_ := 1
  keep_alive_/Duration?
  last_sent_us_/int := ?

  connected_/monitor.Latch ::= monitor.Latch
  pending_/Map/*<int, monitor.Latch>*/ ::= {:}
  incoming_ ::= monitor.Channel 8

  constructor
      client_id/string
      .transport_
      --logger=log.default
      --username/string?=null
      --password/string?=null
      --keep_alive/Duration=DEFAULT_KEEP_ALIVE
      --will/WillConfig?=null:
    keep_alive_ = keep_alive
    logger_ = logger
    // Initialize with the current time.
    // We are doing a connection request just below.
    last_sent_us_ = Time.monotonic_us

    task_ = task --background::
      try:
        catch --trace:
          run_
      finally:
        task_ = null
        close

    connect := ConnectPacket client_id --username=username --password=password --keep_alive=keep_alive --will=will
    transport_.send connect
    ack/ConnAckPacket := connected_.get
    if ack.return_code != 0:
      close
      throw "connection refused: $ack.return_code"

  /**
  Close the MQTT Client.
  */
  close:
    // TODO(anders): This can block, fix me.
    incoming_.send null
    pending_.do --values: it.set null
    if task_:
      task_.cancel
      task_ = null

  send_ packet/Packet:
    transport_.send packet
    last_sent_us_ = Time.monotonic_us

  /**
  Publish a MQTT message on $topic.
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false:
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
      if not ack: throw "client closed"

  /**
  Subscribe to a single topic $filter, with the provided $qos.
  */
  subscribe filter/string --qos/int:
    subscribe [TopicFilter filter --qos=qos]

  /**
  Subscribe to a list a $topic_filters.

  Each topic filter has its own QoS, that the server will verify
    before returning.
  */
  subscribe topic_filters/List:
    packet_id := next_packet_id_++

    packet := SubscribePacket
      topic_filters
      --packet_id=packet_id

    wait_for_ack_ packet_id: | latch/monitor.Latch |
      send_ packet
      ack := latch.get
      if not ack: throw "client closed"

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

  wait_for_ack_ packet_id [block]:
    latch := monitor.Latch
    pending_[packet_id] = latch
    try:
      block.call latch
    finally:
      pending_.remove packet_id

  run_:
    while true:
      remaining_keep_alive_us := keep_alive_.in_us - (Time.monotonic_us - last_sent_us_)
      packet := ?
      if remaining_keep_alive_us <= 0:
        packet = null
      else:
        remaining_keep_alive := Duration --us=remaining_keep_alive_us
        // Timeout returns a `null` packet.
        packet = transport_.receive --timeout=remaining_keep_alive

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
      else:
        throw "unhandled packet type: $packet.type"
