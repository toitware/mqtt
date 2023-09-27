// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import log
import net
import tls
import .full_client
import .session_options
import .last_will
import .packets
import .tcp // For toitdoc.
import .transport
import .topic_qos
import .topic_tree

class Client:
  client_ /FullClient

  subscription_callbacks_ /TopicTree := TopicTree
  logger_ /log.Logger

  /**
  Constructs a new routing MQTT client.

  The $transport parameter is used to send messages and is usually a TCP socket instance.
    See $(constructor --host), $Client.tls, and $TcpTransport.

  The $routes must be a map from topic (of type $string) to callback. After the client started, it
    will automatically subscribe to all topics in the map (with a max-qos of 1). If the broker already
    has a session for this client (which can only happen if the $SessionOptions.clean_session flag
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
      max_qos := 1
      subscription_callbacks_.set topic (CallbackEntry_ callback max_qos)

  /**
  Variant of $(constructor --transport) that connects to the given $host:$port over TCP.
  */
  constructor
      --host /string
      --port /int = 1883
      --net_open /Lambda? = (:: net.open)
      --logger /log.Logger = log.default
      --routes /Map = {:}:
    transport := TcpTransport --host=host --port=port --net_open=net_open
    return Client --transport=transport --logger=logger --routes=routes

  /**
  Variant of $(constructor --host) that supports TLS.
  */
  constructor.tls
      --host /string
      --port /int = 8883
      --net_open /Lambda? = (:: net.open)
      --root_certificates /List = []
      --server_name /string? = null
      --certificate /tls.Certificate? = null
      --logger /log.Logger = log.default
      --routes /Map = {:}:
    transport := TcpTransport.tls --host=host --port=port --net_open=net_open
          --root_certificates=root_certificates
          --server_name=server_name
          --certificate=certificate
    return Client --transport=transport --logger=logger --routes=routes

  /**
  Variant of $(start --options).

  Starts the client with default session options.
  If $client_id is given, uses it as the client ID. Otherwise, changes the
    'clean_session' flag of the options to true, and lets the broker choose a
    fresh client ID.
  */
  start -> none
      --client_id /string = ""
      --background /bool = false
      --on_error /Lambda = (:: throw it)
      --reconnection_strategy /ReconnectionStrategy? = null
      --catch_all_callback /Lambda? = null:
    clean_session := client_id == ""
    options := SessionOptions --client_id=client_id --clean_session=clean_session
    start --options=options
        --background=background
        --on_error=on_error
        --reconnection_strategy=reconnection_strategy
        --catch_all_callback=catch_all_callback

  /**
  Starts the client with the given $options.

  If $background is true, then the handler task won't keep the program running
    if it is the only task left.

  The $on_error callback is called when an error occurs.
    The error is passed as the first argument to the callback.

  The $catch_all_callback is called when a message is received for a topic for which
    no callback is registered.

  If a $reconnection_strategy is provided, uses it for reconnection attempts.
    Otherwise, uses the $TenaciousReconnectionStrategy with a delay of the number
    of attempts in seconds.
  */
  start -> none
      --options /SessionOptions
      --background /bool = false
      --on_error /Lambda = (:: throw it)
      --reconnection_strategy /ReconnectionStrategy? = null
      --catch_all_callback /Lambda? = null:
    reconnection_strategy = reconnection_strategy or
        TenaciousReconnectionStrategy --logger=logger_ --delay_lambda=:: Duration --s=it
    client_.connect --options=options --reconnection_strategy=reconnection_strategy
    task --background=background::
      exception := catch --trace:
        client_.handle: handle_packet_ it --catch_all_callback=catch_all_callback
      if exception: on_error.call exception
    client_.when_running:
      subscribe_all_callbacks_

  /**
  Subscribes to all callbacks in the topic tree.
  */
  subscribe_all_callbacks_ -> none:
    subscription_callbacks_.do: | topic/string callback_entry/CallbackEntry_ |
      subscribe topic callback_entry.callback --max_qos=callback_entry.max_qos

  handle_packet_ packet/Packet --catch_all_callback/Lambda?:
    // We ack the packet as soon as we call the registered callback.
    // This ensures that packets are acked in order (as required by the MQTT protocol).
    // It does not guarantee that the packet was correctly handled. If the callback
    // throws, the packet is not handled again.
    client_.ack packet

    if packet is PublishPacket:
      publish := packet as PublishPacket
      topic := publish.topic
      payload := publish.payload
      was_executed := false
      subscription_callbacks_.do --most_specialized topic: | callback_entry |
        callback_entry.callback.call topic payload
        was_executed = true
      if not was_executed:
        // This can happen when the user unsubscribed from this topic but the
        // packet was already in the incoming queue.
        if catch_all_callback:
          catch_all_callback.call topic payload
        else:
          logger_.info "received packet for unregistered topic $topic"
      return

    if packet is SubAckPacket:
      suback := packet as SubAckPacket
      if (suback.qos.any: it == SubAckPacket.FAILED_SUBSCRIPTION_QOS):
        logger_.error "at least one subscription failed"

    // Ignore all other packets.

  /** Whether the client is closed. */
  is_closed -> bool:
    return client_.is_closed

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
    client_.publish topic payload --qos=qos --retain=retain

  /**
  Subscribes to a single $topic, with the provided $max_qos.

  The chosen $max_qos is the maximum QoS the client will receive. The broker
    generally sends a packet to subscribers with the same QoS as the one it
    received it with. The $max_qos parameter sets a limit on which QoS the client
    wants to receive.

  See $publish for an explanation of the different QoS values.

  The $callback will be called with the topic and the payload of received messages.

  If the client is already connected, still sends another request to the broker to
    subscribe to the topic. This can be useful to obtain retained packets.
  */
  subscribe topic/string --max_qos/int=1 callback/Lambda:
    topics := [ TopicQos topic --max_qos=max_qos ]
    if topics.is_empty: throw "INVALID_ARGUMENT"
    callback_entry := CallbackEntry_ callback max_qos
    subscription_callbacks_.set topic callback_entry
    client_.subscribe_all topics

  /**
  Unsubscribes from a single $topic.

  If the broker has a subscription (see $subscribe) of the given $topic, it will be removed.

  The client removes the callback immediately. If messages for this topic were in transit, they
    are only caught by the catch-all handler.
  */
  unsubscribe topic/string -> none:
    unsubscribe_all [topic]

  /**
  Unsubscribes from all $topics in the given list.

  For each topic in the list of $topics, the broker checks whether it has a
    a subscription (see $subscribe), and removes it if it exists.

  The client removes the callback immediately. If messages for this topic were in transit, they
    are only caught by the catch-all handler.
  */
  unsubscribe_all topics/List -> none:
    client_.unsubscribe_all topics
    topics.do:
      subscription_callbacks_.remove it

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
  max_qos /int

  constructor .callback .max_qos:
