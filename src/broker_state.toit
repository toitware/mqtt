// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .errors
import .link
import .packets
import .payload-store
import .topics

/** Explicit limits for an embedded broker. All counts include offline state. */
class BrokerLimits:
  connections/int
  sessions/int
  subscriptions/int
  filter-bytes/int
  queued-per-session/int
  deliveries/int
  retained/int
  payload-bytes/int
  packet-bytes/int
  topic-bytes/int
  client-id-bytes/int
  session-expiry/Duration
  handshake-timeout/Duration
  write-timeout/Duration
  idle-timeout/Duration
  constructor --.connections=4 --.sessions=8 --.subscriptions=16
      --.filter-bytes=2048 --.queued-per-session=16 --.deliveries=64
      --.retained=16 --.payload-bytes=32_768 --.packet-bytes=4096
      --.topic-bytes=256 --.client-id-bytes=128
      --.session-expiry=(Duration --h=1)
      --.handshake-timeout=(Duration --s=5)
      --.write-timeout=(Duration --s=5)
      --.idle-timeout=(Duration --s=300):
    [connections, sessions, subscriptions, filter-bytes, queued-per-session,
      deliveries, retained, payload-bytes, packet-bytes, topic-bytes, client-id-bytes].do:
      if it < 1: throw "INVALID_ARGUMENT"
    if queued-per-session > 0xffff: throw "INVALID_ARGUMENT"
    if session-expiry.in-us < 0 or handshake-timeout.in-us <= 0 or write-timeout.in-us <= 0 or idle-timeout.in-us <= 0:
      throw "INVALID_ARGUMENT"

class Delivery_:
  key/int
  topic/string
  qos/int
  retain/bool
  id/int?
  sent-generation/int := -1
  constructor .key .topic .qos --.retain=false --.id=null:

class BrokerSession_:
  id/string
  clean/bool
  filters/Map := {:}
  deliveries/List := []
  connection/PacketConnection_? := null
  generation/int := 0
  next-id/int := 1
  disconnected-at/int := 0
  will/Delivery_? := null
  constructor .id .clean:

/**
Owns routing metadata, payload references, and atomic resource admission.

No socket operation runs under this monitor. Storage operations are serialized
  here; the backend must not invoke MQTT methods. A subscriber cannot block a
  publisher by doing network I/O while holding the state lock.
*/
monitor BrokerState_:
  limits_/BrokerLimits
  pool_/PayloadPool_
  sessions_/Map := {:}
  retained_/Map := {:}
  delivery-count_/int := 0
  anonymous_/int := 0
  dropped-wills/int := 0
  constructor .limits_ store/PayloadStore:
    pool_ = PayloadPool_ store limits_.payload-bytes

  previous id/string -> PacketConnection_?:
    session := sessions_.get id
    return session and session.connection

  publish-from session/BrokerSession_ generation/int packet/PublishPacket:
    check-current_ session generation
    publish_ packet

  attach request/ConnectPacket connection/PacketConnection_ now/int -> List:
    expire_ now
    id := request.client-id
    if id.size > limits_.client-id-bytes: throw (CapacityError "client identifier")
    if id == "":
      while true:
        id = "anonymous-$(anonymous_++)"
        if not sessions_.contains id: break
    session/BrokerSession_? := sessions_.get id
    old/PacketConnection_? := session and session.connection
    present := session != null and not request.clean-session and not session.clean
    if session:
      session.generation++
      session.connection = null
      session.deliveries.copy.do: | delivery/Delivery_ |
        if delivery.qos == 0: remove-delivery_ session delivery
      finish-will_ session --publish=true
      if not present:
        discard_ session
        session = null
    created := not session
    if not session:
      if sessions_.size >= limits_.sessions: throw (CapacityError "sessions")
      session = BrokerSession_ id request.clean-session
      sessions_[id] = session
    success := false
    try:
      if request.last-will:
        will := request.last-will
        check-topic_ will.topic
        key := pool_.add will.payload 1
        session.will = Delivery_ key will.topic will.qos --retain=will.retain
      session.connection = connection
      session.generation++
      success = true
      return [session, session.generation, present, old]
    finally:
      if not success and created: discard_ session

  detach session/BrokerSession_ generation/int now/int --graceful/bool:
    if session.generation != generation: return
    session.connection = null
    session.disconnected-at = now
    finish-will_ session --publish=(not graceful)
    // QoS 0 messages do not survive loss of their connection.
    session.deliveries.copy.do: | delivery/Delivery_ |
      if delivery.qos == 0: remove-delivery_ session delivery
    if session.clean: discard_ session

  finish-will_ session/BrokerSession_ --publish/bool:
    will := session.will
    if not will: return
    session.will = null
    try:
      if publish:
        packet := PublishPacket will.topic (pool_.get will.key) --qos=will.qos --retain=will.retain --packet-id=(will.qos == 1 ? 1 : null)
        failure := catch: publish_ packet --existing-key=will.key
        if failure:
          if failure is not CapacityError: throw failure
          // A disconnected publisher cannot retry its will. This loss is observable.
          dropped-wills++
    finally:
      pool_.release will.key

  publish packet/PublishPacket:
    publish_ packet

  publish_ packet/PublishPacket --existing-key/int?=null:
    check-topic_ packet.topic
    recipients := []
    sessions_.do --values: | session/BrokerSession_ |
      qos := -1
      session.filters.do: | filter/string max-qos/int |
        if matches filter packet.topic: qos = max qos (min packet.qos max-qos)
      if qos < 0 or (qos == 0 and not session.connection): continue.do
      check-deliveries_ session 1
      recipients.add [session, qos]
    if delivery-count_ + recipients.size > limits_.deliveries: throw (CapacityError "deliveries")
    keep := packet.retain and packet.payload.size > 0
    if keep and not retained_.contains packet.topic and retained_.size >= limits_.retained:
      throw (CapacityError "retained topics")
    references := recipients.size + (keep ? 1 : 0)
    key/int? := null
    // Preflight all recipients before storing or changing any ownership.
    if references > 0:
      if existing-key:
        key = existing-key
        references.repeat: pool_.retain key
      else:
        key = pool_.add packet.payload references
    recipients.do: | entry |
      session/BrokerSession_ := entry[0]
      qos/int := entry[1]
      delivery := Delivery_ key packet.topic qos --id=(qos == 1 ? allocate-id_ session : null)
      session.deliveries.add delivery
      delivery-count_++
    if packet.retain:
      old/Delivery_? := retained_.get packet.topic
      if keep: retained_[packet.topic] = Delivery_ key packet.topic packet.qos --retain
      else: retained_.remove packet.topic
      if old: pool_.release old.key

  subscribe session/BrokerSession_ generation/int packet/SubscribePacket -> List:
    check-current_ session generation
    results := []
    packet.topics.do: | topic-qos |
      failure := catch: subscribe_ session topic-qos.topic topic-qos.max-qos
      if failure:
        if failure is not CapacityError: throw failure
        results.add 0x80
      else:
        results.add topic-qos.max-qos
    return results

  subscribe_ session/BrokerSession_ filter/string qos/int:
    check-topic_ filter
    if not session.filters.contains filter:
      if session.filters.size >= limits_.subscriptions: throw (CapacityError "subscriptions")
      size := filter.size
      session.filters.do --keys: size += it.size
      if size > limits_.filter-bytes: throw (CapacityError "subscription bytes")
    retained := retained_.values.filter: matches filter it.topic
    check-deliveries_ session retained.size
    retained.do: | entry/Delivery_ |
      delivery-qos := min qos entry.qos
      delivery := Delivery_ entry.key entry.topic delivery-qos --retain
          --id=(delivery-qos == 1 ? allocate-id_ session : null)
      pool_.retain entry.key
      session.deliveries.add delivery
      delivery-count_++
    session.filters[filter] = qos

  unsubscribe session/BrokerSession_ generation/int packet/UnsubscribePacket:
    check-current_ session generation
    packet.topics.do: session.filters.remove it

  next session/BrokerSession_ generation/int -> List?:
    await: session.generation != generation or not session.connection or
        (session.deliveries.any: it.sent-generation != generation)
    if session.generation != generation or not session.connection: return null
    delivery/Delivery_ := (session.deliveries.filter: it.sent-generation != generation).first
    duplicate := delivery.sent-generation != -1 and delivery.qos == 1
    delivery.sent-generation = generation
    payload := pool_.get delivery.key
    packet := PublishPacket delivery.topic payload --qos=delivery.qos --retain=delivery.retain
        --packet-id=delivery.id
        --duplicate=duplicate
    return [delivery, packet]

  sent session/BrokerSession_ generation/int delivery/Delivery_:
    if session.generation != generation: return
    if delivery.qos == 0 and session.deliveries.contains delivery:
      remove-delivery_ session delivery

  ack session/BrokerSession_ generation/int packet/PubAckPacket:
    check-current_ session generation
    found := session.deliveries.filter: it.id == packet.packet-id
    delivery := found.is-empty ? null : found.first
    if delivery and delivery.sent-generation == generation:
      remove-delivery_ session delivery

  check-current_ session/BrokerSession_ generation/int:
    if session.generation != generation or not session.connection: throw (ConnectionError "REPLACED_CONNECTION" null)

  check-topic_ topic/string:
    if topic.size > limits_.topic-bytes: throw (CapacityError "topic bytes")

  check-deliveries_ session/BrokerSession_ count/int:
    if session.deliveries.size + count > limits_.queued-per-session or delivery-count_ + count > limits_.deliveries:
      throw (CapacityError "queued deliveries")

  allocate-id_ session/BrokerSession_ -> int:
    0xffff.repeat:
      id := session.next-id
      session.next-id = id == 0xffff ? 1 : id + 1
      if not (session.deliveries.any: it.id == id): return id
    throw (CapacityError "packet identifiers")

  remove-delivery_ session/BrokerSession_ delivery/Delivery_:
    pool_.release delivery.key
    session.deliveries.remove delivery
    delivery-count_--

  discard_ session/BrokerSession_:
    session.deliveries.copy.do: remove-delivery_ session it
    sessions_.remove session.id

  expire_ now/int:
    sessions_.values.do: | session/BrokerSession_ |
      if not session.connection and now - session.disconnected-at >= limits_.session-expiry.in-us:
        discard_ session

  expire now/int:
    expire_ now

  stats -> Map:
    return {"sessions": sessions_.size, "deliveries": delivery-count_, "retained": retained_.size,
      "payload-bytes": pool_.bytes, "dropped-wills": dropped-wills}

  close:
    sessions_.do --values:
      it.connection = null
    sessions_.clear
    retained_.clear
    delivery-count_ = 0
    pool_.close
