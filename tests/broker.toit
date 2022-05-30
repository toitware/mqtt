// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple MQTT broker library.

This implementation is intended to be used for testing.
*/

import reader
import writer
import log
import .log
import mqtt.packets as mqtt
import mqtt.topic_tree_ as mqtt
import mqtt

interface Transport implements reader.Reader:
  write bytes/ByteArray -> int
  read -> ByteArray?
  close -> none

interface ServerTransport:
  listen callback/Lambda -> none
  close -> none

class Connection_:
  transport_ /Transport
  reader_ /reader.BufferedReader
  writer_ /writer.Writer

  constructor .transport_:
    reader_ = reader.BufferedReader transport_
    writer_ = writer.Writer transport_

  read -> mqtt.Packet?:
    return mqtt.Packet.deserialize reader_

  write packet/mqtt.Packet:
    writer_.write packet.serialize

  close -> none:
    transport_.close

monitor QueuedMessages_:
  // Messages that haven't been sent yet.
  queued_ /Deque := Deque
  // Acks that haven't been sent yet.
  queued_acks_ /Deque := Deque

  next -> mqtt.Packet?:
    await: not queued_.is_empty or not queued_acks_.is_empty
    if not queued_acks_.is_empty: return queued_acks_.remove_first
    return queued_.remove_first

  add_ack packet/mqtt.Packet:
    queued_acks_.add packet

  add_packet packet/mqtt.Packet:
    queued_.add packet

  size -> int:
    return queued_.size + queued_acks_.size

class Session:
  static STATE_CREATED_ ::= 0
  static STATE_RUNNING_ ::= 1
  static STATE_DISCONNECTED_  ::= 2
  // Note that a state can go from disconnected to running again.
  state_ /int := STATE_CREATED_

  client_id /string
  broker /Broker
  clean_session /bool
  logger_ /log.Logger

  subscription_tree_ /mqtt.TopicTree ::= mqtt.TopicTree
  connection_ /Connection_? := null
  reader_task_ /Task_? := null
  writer_task_ /Task_? := null

  queued_ /QueuedMessages_ ::= QueuedMessages_

  // Messages that have been sent but not yet acknowledged.
  waiting_for_ack_ /Map := {:}

  last_will_ /mqtt.LastWill? := null

  next_packet_id_ := 0

  constructor .client_id --logger/log.Logger --.broker/Broker --.clean_session/bool:
    logger_ = logger

  run connection/Connection_ --keep_alive/Duration --last_will/mqtt.LastWill?:
    // Note that there could be a race condition here:
    // If there are multiple requests for the same client, then we might be
    // in the process of closing, while another task enters here.
    if state_ == STATE_RUNNING_: disconnect --reason="already running"

    state_ = STATE_RUNNING_
    last_will_ = last_will

    connection_ = connection
    reader_task_ = task::
      exception := catch --trace:
        while true:
          packet := null
          if keep_alive == (Duration --s=0):
            packet = connection.read
          else:
            with_timeout --ms=(keep_alive.in_ms * 2):
              packet = connection.read
          if not packet: break
          logger_.debug "received $(stringify_packet packet) from client $client_id"
          try:
            handle packet
          finally: | is_exception _ |
            if is_exception:
              logger_.error "error handling packet $(stringify_packet packet)"

      disconnect --reason=exception

    writer_task_ = task::
      exception := catch --trace:
        if not waiting_for_ack_.is_empty:
          waiting_for_ack_.do --values: | packet/mqtt.PublishPacket |
            duped := packet.with --duplicate=true
            connection_.write duped

        while true:
          packet := queued_.next
          logger_.debug "writing $(stringify_packet packet)"
          if packet is mqtt.PublishPacket:
            publish := packet as mqtt.PublishPacket
            if publish.qos > 0:
              waiting_for_ack_[publish.packet_id] = publish

          connection_.write packet
      logger_.info "client $client_id writer task closed with $queued_.size messages pending"

  handle packet/mqtt.Packet:
    if packet is mqtt.SubscribePacket:
      subscribe_packet := packet as mqtt.SubscribePacket
      subscribe subscribe_packet
      return

    if packet is mqtt.UnsubscribePacket:
      unsubscribe_packet := packet as mqtt.UnsubscribePacket
      unsubscribe unsubscribe_packet
      return

    if packet is mqtt.PublishPacket:
      publish_packet := packet as mqtt.PublishPacket
      publish publish_packet
      return

    if packet is mqtt.PingReqPacket:
      ping
      return

    if packet is mqtt.DisconnectPacket:
      disconnect
      return

    logger_.warn "Unhandled packet $(stringify_packet packet)"

  subscribe packet/mqtt.SubscribePacket:
    result_qos := []
    allow_plus := true
    last_was_plus := false
    packet.topic_filters.do: | topic_filter/mqtt.TopicFilter |
      filter := topic_filter.filter
      for i := 0; i < filter.size; i++:
        char := filter[i]
        if not char: continue  // Unicode character.
        if last_was_plus and char != '/': throw "INVALID_SUBSCRIPTION: $filter"
        if char == '+':
          if not allow_plus: throw "INVALID_SUBSCRIPTION: $filter"
          else: last_was_plus = true
        else:
          last_was_plus = false
        allow_plus = char == '/'

        if char == '#' and i != filter.size - 1:
          throw "INVALID_SUBSCRIPTION: $filter"

      if not 0 <= topic_filter.max_qos <= 2:
        throw "INVALID_SUBSCRIPTION: $filter ($topic_filter.max_qos)"

      accepted_qos := min topic_filter.max_qos 1
      subscription_tree_.set filter accepted_qos
      result_qos.add accepted_qos
    send_ (mqtt.SubAckPacket --qos=result_qos --packet_id=packet.packet_id)

    packet.topic_filters.do: | topic_filter/mqtt.TopicFilter |
      filter := topic_filter.filter
      broker.retained.do filter --all: | retained/mqtt.PublishPacket |
        qos := min topic_filter.max_qos  retained.qos
        packet_id := qos > 0 ? next_packet_id_++ : null
        send_ (retained.with --packet_id=packet_id --retain --qos=qos)

  unsubscribe packet/mqtt.UnsubscribePacket:
    packet.topic_filters.do: | topic |
      existed := subscription_tree_.remove topic
      if not existed:
        logger_.info "client $client_id unsubscribed from non-existent topic $topic"

  publish packet/mqtt.PublishPacket:
    topic := packet.topic
    if topic == "" or topic.contains "#" or topic.contains "+":
      throw "INVALID PUBLISH TOPIC. NO WILD CARDS ALLOWED. $packet.topic"
    needs_ack := packet.qos > 0
    if needs_ack:
      packet_id := packet.packet_id
      send_ack_ (mqtt.PubAckPacket packet_id)

    broker.publish packet

  ping:
    send_ (mqtt.PingRespPacket)

  disconnect --reason=null -> none:
    if state_ == STATE_DISCONNECTED_: return
    state_ = STATE_DISCONNECTED_
    reason_suffix := reason ? " ($reason)" :""
    logger_.info "client $client_id closing$reason_suffix"
    if connection_:
      connection_.close
      connection_ = null
    if reader_task_:
      reader_task_.cancel
      reader_task_ = null
    if writer_task_:
      writer_task_.cancel
      writer_task_ = null
    if reason and last_will_:
        packet_id := last_will_.qos > 0 ? next_packet_id_++ : null
        packet := mqtt.PublishPacket last_will_.topic last_will_.payload \
            --qos=last_will_.qos --packet_id=packet_id --retain=last_will_.retain
        broker.publish packet

    if clean_session: broker.remove_session client_id

  send_ packet/mqtt.Packet:
    queued_.add_packet packet

  send_ack_ ack/mqtt.Packet:
    queued_.add_ack ack

  dispatch_incoming_publish packet/mqtt.PublishPacket:
    // There doesn't seem to be a rule which qos we should use if multiple
    // subscriptions match. We thus use the one from the most specialized.
    subscription_tree_.do --most_specialized packet.topic: | subscription_max_qos |
      qos := min packet.qos subscription_max_qos
      packet_id := qos > 0 ? next_packet_id_++ : null
      send_ (packet.with --packet_id=packet_id --qos=qos)

class Broker:
  sessions_ /Map ::= {:}
  server_transport_ /ServerTransport
  logger_ /log.Logger

  retained /mqtt.TopicTree ::= mqtt.TopicTree

  constructor .server_transport_ --logger/log.Logger=log.default:
    logger_ = logger

  start:
    logger_.info "starting broker"
    server_transport_.listen::
      logger_.info "listening"
      connection := Connection_ it
      exception := catch --trace:
        packet := connection.read
        logger_.info "read packet $(stringify_packet packet)"
        if packet is not mqtt.ConnectPacket:
          logger_.error "didn't receive connect packet, but got packet of type $packet.type"
          connection.close
          continue.listen

        connect := packet as mqtt.ConnectPacket

        logger_.info "new connection-request: $(stringify_packet connect)"

        client_id := connect.client_id
        connack /mqtt.ConnAckPacket ::= ?

        clean_session := connect.clean_session
        if client_id == "": client_id = "unknown-$(random)"
        session_present /bool ::= ?
        session /Session? := sessions_.get connect.client_id
        if session and (clean_session or session.clean_session):
          logger_.info "removing existing session for client $client_id"
          session.disconnect
          session = null
        if session:
          logger_.info "existing session for client $client_id"
          session_present = true
        else:
          logger_.info "new session for client $client_id"
          session = Session client_id --broker=this --logger=logger_ --clean_session=clean_session
          sessions_[connect.client_id] = session
          session_present = false

        session.run connection --keep_alive=connect.keep_alive --last_will=connect.last_will
        connack = mqtt.ConnAckPacket --session_present=session_present --return_code=0x00

        connection.write connack
        if connack.return_code != 0:
          connection.close
        else:

  publish packet/mqtt.PublishPacket:
    if packet.retain:
      if packet.payload.size == 0: retained.remove packet.topic
      else: retained.set packet.topic packet
      packet = packet.with --no-retain

    sessions_.do  --values: | session |
      session.dispatch_incoming_publish packet

  remove_session client_id/string:
    sessions_.remove client_id