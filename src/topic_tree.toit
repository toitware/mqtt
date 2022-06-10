// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A tree of topics-segments, matching a topic to a value.
*/
class TopicTree:
  root_ /TopicTreeNode_ := TopicTreeNode_ "ignored_root"

  is_empty -> bool:
    return root_.children.is_empty

  /**
  Inserts, or replaces the value for the given topic.

  Returns the old value. Null if there was none.
  */
  set topic/string value/Object -> any:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_segments := topic.split "/"
    node /TopicTreeNode_ := root_
    topic_segments.do: | segment |
      node = node.children.get segment --init=: TopicTreeNode_ segment
    result := node.value_
    node.value_ = value
    return result

  /**
  Removes the value for the given topic.

  Returns the old value. Null if there was none.
  */
  remove topic/string -> any:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_segments := topic.split "/"
    node /TopicTreeNode_? := root_
    // Keep track of the parent node where we can (maybe) remove the child node from.
    // Any parent that has more than one child or has a value must stay.
    parent_to_remove_from /TopicTreeNode_? := null
    topic_level_to_remove /string? := null
    topic_segments.do: | segment |
      if node == root_ or node.value_ or node.children.size > 1:
        parent_to_remove_from = node
        topic_level_to_remove = segment

      node = node.children.get segment --if_absent=: return null

    result := node.value_
    if node.children.is_empty:
      parent_to_remove_from.children.remove topic_level_to_remove
    else:
      node.value_ = null

    return result

  /**
  Calls $block for each topic and entry.
  */
  do [block] -> none:
    root_.children.do: | segment/string node/TopicTreeNode_ |
      do_all_ segment node block

  do_all_ prefix/string node/TopicTreeNode_ [block] -> none:
    if node.value_: block.call prefix node.value_
    node.children.do: | segment/string child/TopicTreeNode_ |
      do_all_ "$prefix/$segment" child block

  /**
  Calls $block on the most specialized entry that matches the given $topic.

  If none matches does not call the $block.
  */
  do --most_specialized topic/string [block]:
    if not most_specialized: throw "INVALID_ARGUMENT"
    if topic == "": throw "INVALID_ARGUMENT"
    topic_segments := topic.split "/"
    node /TopicTreeNode_? := root_
    catch_all_node /TopicTreeNode_? := null
    topic_segments.do: | segment |
      new_catch_all_node := node.children.get "#"
      if new_catch_all_node: catch_all_node = new_catch_all_node

      new_node := node.children.get segment
      if not new_node: new_node = node.children.get "+"
      if not new_node and not catch_all_node: return
      if not new_node and catch_all_node:
        block.call catch_all_node.value_
        return
      node = new_node
    if node.value_: block.call node.value_
    else if catch_all_node: block.call catch_all_node.value_

  /**
  Calls $block on all entries that match the given $topic.

  If none matches does not call the $block.
  */
  do --all topic/string [block]:
    if topic == "": throw "INVALID_ARGUMENT"
    topic_segments := topic.split "/"
    do_ topic_segments 0 root_ block

  do_ topic_segments/List index/int node/TopicTreeNode_? [block]:
    for ; index < topic_segments.size; index++:
      catch_all_node := node.children.get "#"
      if catch_all_node: block.call catch_all_node.value_

      one_level_node := node.children.get "+"
      if one_level_node:
        // Call recursively.
        do_ topic_segments (index + 1) one_level_node block

      node = node.children.get topic_segments[index]
      if not node: return
    if node.value_: block.call node.value_

  stringify [value_stringifier] -> string:
    result := ""
    root_.children.do: | key value |
      result += stringify_ value key 0 value_stringifier
    return result

  stringify_ node name indentation/int [value_stringifier] -> string:
    indentation_str := " " * indentation
    result := "$indentation_str$name"
    if node.value_: result += ": $(value_stringifier.call node.value_)"
    result += "\n"
    node.children.do: | key value |
      result += stringify_ value "$key" (indentation + 2) value_stringifier
    return result

class TopicTreeNode_:
  segment /string
  value_ /any := null
  children /Map ::= {:}  // string -> TopicTreeNode_?

  constructor .segment:
