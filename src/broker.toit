// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple MQTT broker library.

This implementation was created for testing, but is fully functional.
*/

import monitor
import reader
import log
import writer show Writer
import .packets
import .last_will
import .topic_qos
import .topic_tree

/**
The transport interface used by the broker.
*/
interface BrokerTransport implements reader.Reader:
  write bytes/ByteArray -> int
  read -> ByteArray?
  close -> none

/**
The transport interface allowing the broker to listen to incoming connections.
*/
interface ServerTransport:
  /**
  Listens for incoming connections and calls the $callback whenever a client connects.
  Calls the $callback with a $BrokerTransport as argument.
  */
  listen callback/Lambda -> none
  close -> none

/**
A reader that can timeout.
*/
class TimeoutReader_ implements reader.Reader:
  wrapped_ /reader.Reader
  timeout_ /Duration? := null

  constructor .wrapped_:

  read -> ByteArray?:
    if timeout_:
      with_timeout timeout_: return wrapped_.read
    return wrapped_.read

  set_timeout timeout/Duration:
    timeout_ = timeout

class Connection_:
  transport_ /BrokerTransport
  timeout_reader_ /TimeoutReader_
  reader_ /reader.BufferedReader
  writer_ /Writer

  constructor .transport_:
    timeout_reader_ = TimeoutReader_ transport_
    reader_ = reader.BufferedReader timeout_reader_
    writer_ = Writer transport_

  read -> Packet?:
    return Packet.deserialize reader_

  write packet/Packet:
    writer_.write packet.serialize

  close -> none:
    transport_.close

  set_read_timeout duration/Duration:
    timeout_reader_.set_timeout duration


monitor QueuedMessages_:
  // Messages that haven't been sent yet.
  queued_ /Deque := Deque
  // Acks that haven't been sent yet.
  queued_acks_ /Deque := Deque

  next -> Packet?:
    await: not queued_.is_empty or not queued_acks_.is_empty
    if not queued_acks_.is_empty: return queued_acks_.remove_first
    return queued_.remove_first

  add_ack packet/Packet:
    queued_acks_.add packet

  add_packet packet/Packet:
    queued_.add packet

  size -> int:
    return queued_.size + queued_acks_.size

/**
A client session.

This object keeps track of all the data needed to maintain a session state for
  each client.

If the client connects with a 'clean_session' flag, then this object only stays
  alive until the client disconnects. Otherwise, it is kept forever.

If a client subscribes to messages, but never connects again, the data is just
  saved in the session. Eventually, the broker will run out of memory.
*/
class Session_:
  static STATE_CREATED_ ::= 0
  static STATE_RUNNING_ ::= 1
  static STATE_DISCONNECTED_  ::= 2
  // Note that a state can go from disconnected to running again.
  state_ /int := STATE_CREATED_

  client_id /string
  broker /Broker
  clean_session /bool
  logger_ /log.Logger

  subscription_tree_ /TopicTree ::= TopicTree
  connection_ /Connection_? := null
  // TODO(florian): the following fields should be typed as `Task?`.
  // However, that class is only available in Toit 2.0.
  reader_task_ /any := null
  writer_task_ /any := null

  queued_ /QueuedMessages_ ::= QueuedMessages_

  // Messages that have been sent but not yet acknowledged.
  waiting_for_ack_ /Map := {:}

  last_will_ /LastWill? := null

  next_packet_id_ := 0

  constructor .client_id --logger/log.Logger --.broker/Broker --.clean_session/bool:
    logger_ = logger

  run connection/Connection_ --keep_alive/Duration --last_will/LastWill?:
    // Note that there could be a race condition here:
    // If there are multiple requests for the same client, then we might be
    // in the process of closing, while another task enters here.
    if state_ == STATE_RUNNING_: disconnect --reason="already running"

    state_ = STATE_RUNNING_
    last_will_ = last_will

    connection_ = connection

    if not keep_alive.is_zero:
      connection.set_read_timeout (keep_alive * 2)

    reader_task_ = task::
      exception := catch --trace:
        while true:
          packet := connection.read
          if not packet and state_ != STATE_DISCONNECTED_:
            logger_.info "client $client_id disconnected"
            disconnect --reason="CLIENT_DISCONNECTED"
            break
          logger_.debug "received $(Packet.debug_string_ packet) from client $client_id"
          try:
            handle packet
          finally: | is_exception _ |
            if is_exception:
              logger_.error "error handling packet $(Packet.debug_string_ packet)"
      if exception: disconnect --reason=exception

    writer_task_ = task::
      exception := catch --trace:
        if not waiting_for_ack_.is_empty:
          waiting_for_ack_.do --values: | packet/PublishPacket |
            duped := packet.with --duplicate=true
            connection_.write duped

        while true:
          packet := queued_.next
          logger_.debug "writing $(Packet.debug_string_ packet)"
          if packet is PublishPacket:
            publish := packet as PublishPacket
            if publish.qos > 0:
              waiting_for_ack_[publish.packet_id] = publish

          connection_.write packet
      logger_.info "client $client_id writer task closed with $queued_.size messages pending"

  handle packet/Packet:
    if packet is SubscribePacket:
      subscribe_packet := packet as SubscribePacket
      subscribe subscribe_packet
      return

    if packet is UnsubscribePacket:
      unsubscribe_packet := packet as UnsubscribePacket
      unsubscribe unsubscribe_packet
      return

    if packet is PublishPacket:
      publish_packet := packet as PublishPacket
      publish publish_packet
      return

    if packet is PingReqPacket:
      ping
      return

    if packet is DisconnectPacket:
      disconnect
      return

    if packet is PubAckPacket:
      id := (packet as PubAckPacket).packet_id
      waiting_for_ack_.remove id

    logger_.warn "Unhandled packet $(Packet.debug_string_ packet)"

  subscribe packet/SubscribePacket:
    result_qos := []
    allow_plus := true
    last_was_plus := false
    packet.topics.do: | topic_qos/TopicQos |
      topic := topic_qos.topic
      for i := 0; i < topic.size; i++:
        char := topic[i]
        if not char: continue  // Unicode character.
        if last_was_plus and char != '/': throw "INVALID_SUBSCRIPTION: $topic"
        if char == '+':
          if not allow_plus: throw "INVALID_SUBSCRIPTION: $topic"
          else: last_was_plus = true
        else:
          last_was_plus = false
        allow_plus = char == '/'

        if char == '#' and i != topic.size - 1:
          throw "INVALID_SUBSCRIPTION: $topic"

      if not 0 <= topic_qos.max_qos <= 2:
        throw "INVALID_SUBSCRIPTION: $topic ($topic_qos.max_qos)"

      accepted_qos := min topic_qos.max_qos 1
      subscription_tree_.set topic accepted_qos
      result_qos.add accepted_qos
    send_ (SubAckPacket --qos=result_qos --packet_id=packet.packet_id)

    packet.topics.do: | topic_qos/TopicQos |
      topic := topic_qos.topic
      broker.retained.do topic --all: | retained/PublishPacket |
        qos := min topic_qos.max_qos  retained.qos
        packet_id := qos > 0 ? next_packet_id_++ : null
        send_ (retained.with --packet_id=packet_id --retain --qos=qos)

  unsubscribe packet/UnsubscribePacket:
    packet.topics.do: | topic |
      existed := subscription_tree_.remove topic
      if not existed:
        logger_.info "client $client_id unsubscribed from non-existent topic $topic"
    send_ (UnsubAckPacket --packet_id=packet.packet_id)

  publish packet/PublishPacket:
    topic := packet.topic
    if topic == "" or topic.contains "#" or topic.contains "+":
      throw "INVALID PUBLISH TOPIC. NO WILD CARDS ALLOWED. $packet.topic"
    needs_ack := packet.qos > 0
    if needs_ack:
      packet_id := packet.packet_id
      send_ack_ (PubAckPacket --packet_id=packet_id)

    broker.publish packet

  ping:
    send_ (PingRespPacket)

  disconnect --reason=null -> none:
    if state_ == STATE_DISCONNECTED_: return
    state_ = STATE_DISCONNECTED_
    reason_suffix := reason ? " ($reason)" :""
    logger_.info "client $client_id closing$reason_suffix"
    if connection_:
      connection_.close
      connection_ = null

    // Send the last will before we kill all tasks.
    // Otherwise we will cancel the task on which we currently run on.
    if reason and last_will_:
      packet_id := last_will_.qos > 0 ? next_packet_id_++ : null
      packet := PublishPacket last_will_.topic last_will_.payload \
          --qos=last_will_.qos --packet_id=packet_id --retain=last_will_.retain
      broker.publish packet

    if clean_session: broker.remove_session_ client_id

    // Cancel the writer_task_ first, since the reader task might be
    // the one calling the disconnect.
    assert: writer_task_ != task
    if writer_task_:
      writer_task_.cancel
      writer_task_ = null
    if reader_task_:
      reader_task := reader_task_
      reader_task_ = null
      reader_task.cancel

  send_ packet/Packet:
    queued_.add_packet packet

  send_ack_ ack/Packet:
    queued_.add_ack ack

  dispatch_incoming_publish packet/PublishPacket:
    // There doesn't seem to be a rule which qos we should use if multiple
    // subscriptions match. We thus use the one from the most specialized.
    subscription_tree_.do --most_specialized packet.topic: | subscription_max_qos |
      qos := min packet.qos subscription_max_qos
      NO_PACKET_ID ::= -1  // See $PublishPacket.with.
      packet_id := qos > 0 ? next_packet_id_++ : NO_PACKET_ID
      send_ (packet.with --packet_id=packet_id --qos=qos)

/** An unbounded channel for publish messages. */
class PublishChannel_:
  // Messages that haven't been sent yet.
  queued_ /Deque := Deque
  semaphore_ /monitor.Semaphore := monitor.Semaphore

  next -> Packet:
    semaphore_.down
    return queued_.remove_first

  add packet/Packet:
    queued_.add packet
    semaphore_.up

/**
An MQTT broker.
*/
class Broker:
  sessions_ /Map ::= {:}
  server_transport_ /ServerTransport
  logger_ /log.Logger
  publish_channel_ /PublishChannel_ ::= PublishChannel_

  retained /TopicTree ::= TopicTree

  constructor .server_transport_ --logger/log.Logger=log.default:
    logger_ = logger

  start:
    logger_.info "starting broker"

    publish_task := task --background::
      while true:
        packet := publish_channel_.next
        sessions_.do  --values: | session |
          session.dispatch_incoming_publish packet

    try:
      server_transport_.listen::
        logger_.info "connection established"
        connection := Connection_ it
        exception := catch --trace:
          packet := connection.read
          if not packet:
            logger_.info "connection was closed"
            connection.close
            continue.listen

          logger_.info "read packet $(Packet.debug_string_ packet)"
          if packet is not ConnectPacket:
            logger_.error "didn't receive connect packet, but got packet of type $packet.type"
            connection.close
            continue.listen

          connect := packet as ConnectPacket

          logger_.info "new connection-request: $(Packet.debug_string_ connect)"

          client_id := connect.client_id
          connack /ConnAckPacket ::= ?

          clean_session := connect.clean_session
          if client_id == "": client_id = "unknown-$(random)"
          session_present /bool ::= ?
          session /Session_? := sessions_.get connect.client_id
          if session and (clean_session or session.clean_session):
            logger_.info "removing existing session for client $client_id"
            session.disconnect
            session = null
          if session:
            logger_.info "existing session for client $client_id"
            session_present = true
          else:
            logger_.info "new session for client $client_id"
            session = Session_ client_id --broker=this --logger=logger_ --clean_session=clean_session
            sessions_[connect.client_id] = session
            session_present = false

          session.run connection --keep_alive=connect.keep_alive --last_will=connect.last_will
          connack = ConnAckPacket --session_present=session_present --return_code=0x00

          connection.write connack

          // Currently we always succeed the connection, so the following 'if' never triggers.
          if connack.return_code != 0:
            connection.close
            continue.listen
    finally:
      publish_task.cancel

  publish packet/PublishPacket:
    logger_.info "publishing $(Packet.debug_string_ packet)"
    if packet.retain:
      if packet.payload.size == 0: retained.remove packet.topic
      else: retained.set packet.topic packet
      packet = packet.with --no-retain

    // Hand over the packet to the publish channel.
    // We can't notify the sessions ourselves as the current task might be killed soon.
    publish_channel_.add packet

  remove_session_ client_id/string:
    sessions_.remove client_id
