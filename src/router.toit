// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import log
import .client
import .client_options
import .last_will
import .packets
import .tcp // For toitdoc.
import .topic_filter
import .transport

class CallbackEntry_:
  callback /Lambda
  max_qos /int
  is_subscribed /bool := true

  constructor .callback .max_qos:

class SubscriptionTreeNode_:
  topic_level /string
  callback_entry_ /CallbackEntry_? := null
  children /Map ::= {:}  // string -> SubscriptionTreeNode_?

  constructor .topic_level:

/**
A tree of subscription, matching a topic to the registered callback.
*/
class SubscriptionTree_:
  root /SubscriptionTreeNode_ := SubscriptionTreeNode_ "ignored_root"

  /**
  Inserts, or replaces the callback for the given topic.

  Returns the old callback entry. Null if there was none.
  */
  add topic/string callback_entry/CallbackEntry_ -> CallbackEntry_?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_ := root
    topic_levels.do: | topic_level |
      node = node.children.get topic_level --init=: SubscriptionTreeNode_ topic_level
    result := node.callback_entry_
    node.callback_entry_ = callback_entry
    return result

  /**
  Removes the callback for the given topic.

  Returns the old callback entry. Null if there was none.
  */
  remove topic/string -> CallbackEntry_?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_? := root
    // Keep track of the parent node where we can (maybe) remove the child node from.
    // Any parent that has more than one child or has a callback must stay.
    parent_to_remove_from /SubscriptionTreeNode_? := root
    topic_level_to_remove /string? := null
    topic_levels.do: | topic_level |
      if node.callback_entry_ or node.children.size > 1:
        parent_to_remove_from = node
        topic_level_to_remove = topic_level

      node = node.children.get topic_level --if_absent=: throw "NOT SUBSCRIBED TO $topic"

    result := node.callback_entry_
    if node.children.is_empty:
      parent_to_remove_from.children.remove topic_level_to_remove
    else:
      node.callback_entry_ = null

    return result

  find topic/string -> CallbackEntry_?:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_levels := topic.split "/"
    node /SubscriptionTreeNode_? := root
    catch_all_callback /CallbackEntry_? := null
    topic_levels.do: | topic_level |
      catch_all_node := node.children.get "#"
      if catch_all_node: catch_all_callback = catch_all_node.callback_entry_

      new_node := node.children.get topic_level
      if not new_node: new_node = node.children.get "+"
      if not new_node and not catch_all_callback: return null
      if not new_node: return catch_all_callback
      node = new_node
    if node.callback_entry_: return node.callback_entry_
    return catch_all_callback

class Router:
  client_ /Client

  subscription_callbacks_ /SubscriptionTree_ := SubscriptionTree_
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
    client_ = Client --transport=transport --logger=logger

  start --attached/bool --session_options /SessionOptions:
    if not attached: throw "INVALID_ARGUMENT"
    client_.connect --options=session_options
    client_.handle: handle_packet_ it

  start -> none
      --detached/bool
      --session_options /SessionOptions
      --background/bool=false
      --on_error/Lambda=(:: throw it):
    if not detached: throw "INVALID_ARGUMENT"
    client_.connect --options=session_options
    task --background=background::
      exception := catch --trace:
        client_.handle: handle_packet_ it
      if exception: on_error.call exception
    client_.when_running: return

  handle_packet_ packet/Packet:
    // We ack the packet as soon as we call the registered callback.
    // This ensures that packets are acked in order (as required by the MQTT protocol).
    // It does not guarantee that the packet was correctly handled. If the callback
    // throws, the packet is not handled again.
    client_.ack packet

    if packet is PublishPacket:
      publish := packet as PublishPacket
      topic := publish.topic
      payload := publish.payload
      callback := subscription_callbacks_.find topic
      if callback:
        callback.callback.call topic payload
      else:
        // This can happen when the user unsubscribed from this topic but the
        // packet was already in the incoming queue.
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

  See $Client.publish.
  */
  publish topic/string payload/ByteArray --qos=1 --retain=false:
    client_.publish topic payload --qos=qos --retain=retain

  /**
  Subscribes to a single topic $filter, with the provided $max_qos.

  The chosen $max_qos is the maximum QoS the client will receive. The broker
    generally sends a packet to subscribers with the same QoS as the one it
    received it with. The $max_qos parameter sets a limit on which QoS the client
    wants to receive.

  See $publish for an explanation of the different QoS values.
  */
  subscribe filter/string --max_qos/int=1 callback/Lambda:
    topic_filters := [ TopicFilter filter --max_qos=max_qos ]
    if topic_filters.is_empty: throw "INVALID_ARGUMENT"

    callback_entry := CallbackEntry_ callback max_qos
    old_entry := subscription_callbacks_.add filter callback_entry
    if old_entry:
      old_entry.is_subscribed = false
      if old_entry.max_qos == max_qos:
      // Just a simple change of callback.
      return

    client_.subscribe_all topic_filters

  /**
  Unsubscribes from a single topic $filter.

  The client must be connected to the $filter.
  */
  unsubscribe filter/string -> none:
    client_.unsubscribe filter
    // TODO(florian): wait for the ack and then remove the callback from the tree.
    // Or maybe remove the callback immediately.

  /**
  Unsubscribes from all $filters in the given list.
  */
  unsubscribe_all filters/List -> none:
    client_.unsubscribe_all filters

  close -> none:
    client_.close
