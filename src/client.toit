// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import log
import net
import tls
import .full-client
import .session-options
import .last-will
import .packets
import .tcp // For toitdoc.
import .transport
import .topic-qos
import .topic-tree

class Client:
  client_ /FullClient

  subscription-callbacks_ /TopicTree := TopicTree
  logger_ /log.Logger

  /**
  Constructs a new routing MQTT client.

  The $transport parameter is used to send messages and is usually a TCP socket instance.
    See $(constructor --host), $Client.tls, and $TcpTransport.

  The $routes must be a map from topic (of type $string) to callback. After the client started, it
    will automatically subscribe to all topics in the map (with a max-qos of 1). If the broker already
    has a session for this client (which can only happen if the $SessionOptions.clean-session flag
    is not set), then the client might receive messages for these topics before there was any time to
    call $subscribe, which is why it's a good idea to set the routes in the constructor.
  */
  constructor
      --transport /Transport
      --logger /log.Logger = log.default
      --routes /Map = {:}:
    logger_ = logger
    client_ = FullClient --transport=transport --logger=logger
    routes.do: | topic/string callback/Lambda |
      max-qos := 1
      subscription-callbacks_.set topic (CallbackEntry_ callback max-qos)

  /**
  Variant of $(constructor --transport) that connects to the given $host:$port over TCP.
  */
  constructor
      --host /string
      --port /int = 1883
      --net-open /Lambda? = (:: net.open)
      --logger /log.Logger = log.default
      --routes /Map = {:}:
    transport := TcpTransport --host=host --port=port --net-open=net-open
    return Client --transport=transport --logger=logger --routes=routes

  /**
  Variant of $(constructor --host) that supports TLS.
  */
  constructor.tls
      --host /string
      --port /int = 8883
      --net-open /Lambda? = (:: net.open)
      --root-certificates /List = []
      --server-name /string? = null
      --certificate /tls.Certificate? = null
      --logger /log.Logger = log.default
      --routes /Map = {:}:
    transport := TcpTransport.tls --host=host --port=port --net-open=net-open
          --root-certificates=root-certificates
          --server-name=server-name
          --certificate=certificate
    return Client --transport=transport --logger=logger --routes=routes

  /**
  Variant of $(start --options).

  Starts the client with default session options.
  If $client-id is given, uses it as the client ID. Otherwise, changes the
    'clean_session' flag of the options to true, and lets the broker choose a
    fresh client ID.
  */
  start -> none
      --client-id /string = ""
      --background /bool = false
      --on-error /Lambda = (:: /* Do nothing. */)
      --reconnection-strategy /ReconnectionStrategy? = null
      --catch-all-callback /Lambda? = null:
    clean-session := client-id == ""
    options := SessionOptions --client-id=client-id --clean-session=clean-session
    start --options=options
        --background=background
        --on-error=on-error
        --reconnection-strategy=reconnection-strategy
        --catch-all-callback=catch-all-callback

  /**
  Starts the client with the given $options.

  If $background is true, then the handler task won't keep the program running
    if it is the only task left.

  The $on-error callback is called when an error occurs.
    The error is passed as the first argument to the callback.

  The $catch-all-callback is called when a message is received for a topic for which
    no callback is registered.

  If a $reconnection-strategy is provided, uses it for reconnection attempts.
    Otherwise, uses the $TenaciousReconnectionStrategy with a delay of the number
    of attempts in seconds.
  */
  start -> none
      --options /SessionOptions
      --background /bool = false
      --on-error /Lambda = (:: /* Do nothing */)
      --reconnection-strategy /ReconnectionStrategy? = null
      --catch-all-callback /Lambda? = null:
    reconnection-strategy = reconnection-strategy or
        TenaciousReconnectionStrategy --logger=logger_ --delay-lambda=:: Duration --s=it
    client_.connect --options=options --reconnection-strategy=reconnection-strategy
    task --background=background::
      exception := catch --trace:
        client_.handle: handle-packet_ it --catch-all-callback=catch-all-callback
      if exception: on-error.call exception
    client_.when-running:
      subscribe-all-callbacks_

  /**
  Subscribes to all callbacks in the topic tree.
  */
  subscribe-all-callbacks_ -> none:
    subscription-callbacks_.do: | topic/string callback-entry/CallbackEntry_ |
      subscribe topic callback-entry.callback --max-qos=callback-entry.max-qos

  handle-packet_ packet/Packet --catch-all-callback/Lambda?:
    // We ack the packet as soon as we call the registered callback.
    // This ensures that packets are acked in order (as required by the MQTT protocol).
    // It does not guarantee that the packet was correctly handled. If the callback
    // throws, the packet is not handled again.
    client_.ack packet

    if packet is PublishPacket:
      publish := packet as PublishPacket
      topic := publish.topic
      payload := publish.payload
      was-executed := false
      subscription-callbacks_.do --most-specialized topic: | callback-entry |
        callback-entry.callback.call topic payload
        was-executed = true
      if not was-executed:
        // This can happen when the user unsubscribed from this topic but the
        // packet was already in the incoming queue.
        if catch-all-callback:
          catch-all-callback.call topic payload
        else:
          logger_.info "received packet for unregistered topic $topic"
      return

    if packet is SubAckPacket:
      suback := packet as SubAckPacket
      if (suback.qos.any: it == SubAckPacket.FAILED-SUBSCRIPTION-QOS):
        logger_.error "at least one subscription failed"

    // Ignore all other packets.

  /** Whether the client is closed. */
  is-closed -> bool:
    return client_.is-closed

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

  The $persistence-token parameter is used when a packet is sent with $qos equal to 1. In this case
    the $PersistedPacket that is given to the $PersistenceStore contains this token. The persistence
    store can use this information to avoid keeping the data in memory, or to clear data from
    the flash.
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false --persistence-token/any=null:
    client_.publish topic payload --qos=qos --retain=retain --persistence-token=persistence-token

  /**
  Subscribes to a single $topic, with the provided $max-qos.

  The chosen $max-qos is the maximum QoS the client will receive. The broker
    generally sends a packet to subscribers with the same QoS as the one it
    received it with. The $max-qos parameter sets a limit on which QoS the client
    wants to receive.

  See $publish for an explanation of the different QoS values.

  The $callback will be called with the topic and the payload of received messages.

  If the client is already connected, still sends another request to the broker to
    subscribe to the topic. This can be useful to obtain retained packets.
  */
  subscribe topic/string --max-qos/int=1 callback/Lambda:
    topics := [ TopicQos topic --max-qos=max-qos ]
    if topics.is-empty: throw "INVALID_ARGUMENT"
    callback-entry := CallbackEntry_ callback max-qos
    subscription-callbacks_.set topic callback-entry
    client_.subscribe-all topics

  /**
  Unsubscribes from a single $topic.

  If the broker has a subscription (see $subscribe) of the given $topic, it will be removed.

  The client removes the callback immediately. If messages for this topic were in transit, they
    are only caught by the catch-all handler.
  */
  unsubscribe topic/string -> none:
    unsubscribe-all [topic]

  /**
  Unsubscribes from all $topics in the given list.

  For each topic in the list of $topics, the broker checks whether it has a
    a subscription (see $subscribe), and removes it if it exists.

  The client removes the callback immediately. If messages for this topic were in transit, they
    are only caught by the catch-all handler.
  */
  unsubscribe-all topics/List -> none:
    client_.unsubscribe-all topics
    topics.do:
      subscription-callbacks_.remove it

  /**
  Closes the client.

  Unless $force is true, just sends a disconnect packet to the broker. The client then
    shuts down gracefully once the broker has closed the connection.

  If $force is true, shuts down the client by severing the transport.
  */
  close --force/bool=false -> none:
    client_.close --force=force

class CallbackEntry_:
  callback /Lambda
  max-qos /int

  constructor .callback .max-qos:
