// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import log
import .full_client
import .client_options
import .last_will
import .packets
import .tcp // For toitdoc.
import .transport
import .topic_qos
import .topic_tree_

class CallbackEntry_:
  callback /Lambda
  max_qos /int
  is_subscribed /bool := true

  constructor .callback .max_qos:

class Router:
  client_ /FullClient

  subscription_callbacks_ /TopicTree := TopicTree
  logger_ /log.Logger?

  /**
  Constructs a new routing MQTT client.

  The $transport parameter is used to send messages and is usually a TCP socket instance.
    See $TcpTransport.
  */
  // TODO(florian): we must be able to set callback handlers before we start the client.
  // Otherwise we will have "Received packet for unregister topics.".
  constructor
      --transport /Transport
      --logger /log.Logger? = log.default:
    logger_ = logger
    client_ = FullClient --transport=transport --logger=logger

  /**
  Variant of $(start --attached --session_options).

  Starts the client with default options.
  If $client_id is given, uses it as the client ID. Otherwise, changes the
    'clean_session' flag of the options to true, and lets the broker chose a
    fresh client ID.
  */
  start -> none
      --attached /bool
      --client_id /string = ""
      --catch_all_callback /Lambda? = null:
    if not attached: throw "INVALID_ARGUMENT"
    clean_session := client_id == ""
    options := SessionOptions --client_id=client_id --clean_session=clean_session
    start --attached --session_options=options --catch_all_callback=catch_all_callback

  start -> none
      --attached /bool
      --session_options /SessionOptions
      --catch_all_callback /Lambda? = null:
    if not attached: throw "INVALID_ARGUMENT"
    client_.connect --options=session_options
    client_.handle: handle_packet_ it --catch_all_callback=catch_all_callback

  /**
  Variant of $(start --detached --session_options).

  Starts the client with default session options.
  If $client_id is given, uses it as the client ID. Otherwise, changes the
    'clean_session' flag of the options to true, and lets the broker chose a
    fresh client ID.
  */
  start -> none
      --detached /bool
      --client_id /string = ""
      --background /bool=false
      --on_error /Lambda=(:: throw it)
      --catch_all_callback /Lambda? = null:
    if not detached: throw "INVALID_ARGUMENT"
    clean_session := client_id == ""
    options := SessionOptions --client_id=client_id --clean_session=clean_session
    start --detached
        --session_options=options
        --background=background
        --on_error=on_error
        --catch_all_callback=catch_all_callback

  start -> none
      --detached /bool
      --session_options /SessionOptions
      --background /bool = false
      --on_error /Lambda = (:: throw it)
      --catch_all_callback /Lambda? = null:
    if not detached: throw "INVALID_ARGUMENT"
    client_.connect --options=session_options
    task --background=background::
      exception := catch --trace:
        client_.handle: handle_packet_ it --catch_all_callback=catch_all_callback
      if exception: on_error.call exception
    client_.when_running: return

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

  is_closed -> bool:
    return client_.is_closed

  /**
  Publishes an MQTT message on $topic.

  See $FullClient.publish.
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
  */
  subscribe topic/string --max_qos/int=1 callback/Lambda:
    topics := [ TopicQos topic --max_qos=max_qos ]
    if topics.is_empty: throw "INVALID_ARGUMENT"

    callback_entry := CallbackEntry_ callback max_qos
    old_entry := subscription_callbacks_.set topic callback_entry
    if old_entry:
      old_entry.is_subscribed = false
      if old_entry.max_qos == max_qos:
      // Just a simple change of callback.
      return

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

  close -> none:
    client_.close
