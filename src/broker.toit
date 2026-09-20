// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .broker-state
import .completion
import .errors
import .link
import .packets
import .payload-store
import .topics
import .wire

export BrokerLimits PayloadStore MemoryPayloadStore

monitor BrokerTasks_:
  limit_/int
  links_/List := []
  stopping/bool := false
  failure/any := null
  client-errors/int := 0
  last-client-error/any := null
  constructor .limit_:
  add link/Link -> bool:
    if stopping or links_.size >= limit_: return false
    links_.add link
    return true
  remove link/Link:
    links_.remove link
  report failure/any:
    client-errors++
    last-client-error = failure
  stop --reason=null -> List:
    stopping = true
    if reason and not failure: failure = reason
    return links_.copy
  wait:
    await: links_.is-empty

/**
An embedded MQTT 3.1.1 broker with explicit resource budgets.

One state monitor owns sessions and payload references. Each accepted connection
  has a reader and writer; neither holds that monitor during socket I/O. Slow or
  invalid clients are disconnected independently. Storage failure stops the broker.

Persistent sessions survive network reconnection, but not broker restart. Expiry
  is applied on admission or explicitly with expire-sessions. All payloads are
  bounded, including retained messages and wills. Capacity exhaustion closes an
  incoming publisher without acknowledging its QoS 1 message. Unpublishable wills
  increment the dropped-wills statistic.
*/
class Broker:
  listener_/Listener
  limits_/BrokerLimits
  state_/BrokerState_
  tasks_/BrokerTasks_
  wire_/Wire
  lifetime_/Completion_ := Completion_
  started_/bool := false
  authenticate_/Lambda?

  constructor listener/Listener --limits/BrokerLimits=BrokerLimits
      --store/PayloadStore=MemoryPayloadStore
      --authenticate/Lambda?=null:
    listener_ = listener
    limits_ = limits
    state_ = BrokerState_ limits store
    tasks_ = BrokerTasks_ limits.connections
    wire_ = Wire --max-packet-size=limits.packet-bytes
    authenticate_ = authenticate

  /** Starts accepting clients. Observe its lifetime with wait-closed. */
  start -> none:
    if started_: throw "ALREADY_STARTED"
    started_ = true
    task --background:: run_

  /** Stops accepting connections and closes all clients. */
  close -> none:
    stop_
    if not started_:
      started_ = true
      failure := catch: state_.close
      lifetime_.complete --failure=(tasks_.failure or failure)

  /** Waits for all connection tasks and storage cleanup; throws terminal failure. */
  wait-closed -> none:
    if not started_: throw "NOT_STARTED"
    lifetime_.wait

  /** Publishes locally, with the same admission rules as a network publisher. */
  publish topic/string payload/ByteArray --qos/int=1 --retain/bool=false -> none:
    validate-topic topic
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"
    if tasks_.stopping: throw (MqttError "BROKER_CLOSED")
    packet := PublishPacket topic payload --qos=qos --retain=retain --packet-id=(qos == 1 ? 1 : null)
    wire_.encode packet
    failure := catch: state_.publish packet
    if failure:
      if failure is StorageError: stop_ --reason=failure
      throw failure

  /** Returns bounded-state counts and client failure diagnostics. */
  stats -> Map:
    result := state_.stats
    result["failure"] = tasks_.failure
    result["client-errors"] = tasks_.client-errors
    result["last-client-error"] = tasks_.last-client-error
    return result

  /** Expires disconnected sessions using a monotonic timestamp. */
  expire-sessions --now/int=Time.monotonic-us:
    failure := catch: state_.expire now
    if failure:
      if failure is StorageError: stop_ --reason=failure
      throw failure

  stop_ --reason=null:
    links := tasks_.stop --reason=reason
    listener_.close
    links.do: it.close

  run_:
    failure := catch:
      while not tasks_.stopping:
        link := listener_.accept
        if not link: break
        if not tasks_.add link:
          link.close
          continue
        serve-background_ link
    if failure and not tasks_.stopping: stop_ --reason=failure
    critical-do:
      stop_
      tasks_.wait
      cleanup-failure := catch: state_.close
      lifetime_.complete --failure=(tasks_.failure or cleanup-failure)

  serve-background_ link/Link:
    task --background::
      failure := catch: serve_ link
      critical-do:
        link.close
        if failure and not tasks_.stopping:
          tasks_.report failure
          if failure is StorageError: stop_ --reason=failure
        tasks_.remove link

  serve_ link/Link:
    connection := PacketConnection_ link wire_ --write-timeout=limits_.write-timeout
    session/BrokerSession_? := null
    generation := 0
    graceful := false
    writer/Task? := null
    writer-failure/any := null
    try:
      request/ConnectPacket? := null
      with-timeout limits_.handshake-timeout:
        packet := connection.read
        if packet is not ConnectPacket: throw (ProtocolError "expected CONNECT")
        request = packet as ConnectPacket
        if authenticate_ and not authenticate_.call request:
          connection.write (ConnAckPacket --return-code=ConnAckPacket.NOT-AUTHORIZED)
          return
      previous := state_.previous request.client-id
      if previous: previous.close
      attached := state_.attach request connection Time.monotonic-us
      session = attached[0]
      generation = attached[1]
      previous = attached[3]
      if previous: previous.close
      connection.write (ConnAckPacket --session-present=attached[2])
      writer = task --background::
        writer-failure = catch:
          while entry := state_.next session generation:
            delivery/Delivery_ := entry[0]
            connection.write entry[1]
            state_.sent session generation delivery
        connection.close
      while true:
        packet/Packet? := null
        timeout := request.keep-alive.is-zero ? limits_.idle-timeout : request.keep-alive * 1.5
        with-timeout timeout: packet = connection.read
        if not packet: break
        if packet is DisconnectPacket:
          graceful = true
          break
        else if packet is PublishPacket:
          publish := packet as PublishPacket
          state_.publish-from session generation publish
          if publish.qos == 1: connection.write (PubAckPacket --packet-id=publish.packet-id)
        else if packet is SubscribePacket:
          subscribe := packet as SubscribePacket
          qos := state_.subscribe session generation subscribe
          connection.write (SubAckPacket --packet-id=subscribe.packet-id --qos=qos)
        else if packet is UnsubscribePacket:
          unsubscribe := packet as UnsubscribePacket
          state_.unsubscribe session generation unsubscribe
          connection.write (UnsubAckPacket --packet-id=unsubscribe.packet-id)
        else if packet is PubAckPacket:
          state_.ack session generation (packet as PubAckPacket)
        else if packet is PingReqPacket:
          connection.write PingRespPacket
        else:
          throw (ProtocolError "unexpected client packet")
    finally:
      critical-do:
        if writer: writer.cancel
        connection.close
        if session:
          state_.detach session generation Time.monotonic-us --graceful=(graceful or tasks_.stopping)
        if writer-failure is StorageError: stop_ --reason=writer-failure
