// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A tree of topics-segments, matching a topic to a value.
*/
class TopicTree:
  root_ /TopicTreeNode_ := TopicTreeNode_ "ignored_root"

  is-empty -> bool:
    return root_.children.is-empty

  /**
  Inserts, or replaces the value for the given topic.

  Returns the old value. Null if there was none.
  */
  set topic/string value/Object -> any:
    if topic == "": throw "INVALID_ARGUMENT"
    topic-segments := topic.split "/"
    node /TopicTreeNode_ := root_
    topic-segments.do: | segment |
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
    topic-segments := topic.split "/"
    node /TopicTreeNode_? := root_
    // Keep track of the parent node where we can (maybe) remove the child node from.
    // Any parent that has more than one child or has a value must stay.
    parent-to-remove-from /TopicTreeNode_? := null
    topic-level-to-remove /string? := null
    topic-segments.do: | segment |
      if node == root_ or node.value_ or node.children.size > 1:
        parent-to-remove-from = node
        topic-level-to-remove = segment

      node = node.children.get segment --if-absent=: return null

    result := node.value_
    if node.children.is-empty:
      parent-to-remove-from.children.remove topic-level-to-remove
    else:
      node.value_ = null

    return result

  /**
  Calls $block for each topic and entry.
  */
  do [block] -> none:
    root_.children.do: | segment/string node/TopicTreeNode_ |
      do-all_ segment node block

  do-all_ prefix/string node/TopicTreeNode_ [block] -> none:
    if node.value_: block.call prefix node.value_
    node.children.do: | segment/string child/TopicTreeNode_ |
      do-all_ "$prefix/$segment" child block

  /**
  Calls $block on the most specialized entry that matches the given $topic.

  If none matches does not call the $block.
  */
  do --most-specialized topic/string [block]:
    if not most-specialized: throw "INVALID_ARGUMENT"
    if topic == "": throw "INVALID_ARGUMENT"
    topic-segments := topic.split "/"
    node /TopicTreeNode_? := root_
    catch-all-node /TopicTreeNode_? := null
    topic-segments.do: | segment |
      new-catch-all-node := node.children.get "#"
      if new-catch-all-node: catch-all-node = new-catch-all-node

      new-node := node.children.get segment
      if not new-node: new-node = node.children.get "+"
      if not new-node and not catch-all-node: return
      if not new-node and catch-all-node:
        block.call catch-all-node.value_
        return
      node = new-node
    if node.value_: block.call node.value_
    else if catch-all-node: block.call catch-all-node.value_

  /**
  Calls $block on all entries that match the given $topic.

  If none matches does not call the $block.
  */
  do --all topic/string [block]:
    if topic == "": throw "INVALID_ARGUMENT"
    topic-segments := topic.split "/"
    do_ topic-segments 0 root_ block

  do_ topic-segments/List index/int node/TopicTreeNode_? [block]:
    for ; index < topic-segments.size; index++:
      catch-all-node := node.children.get "#"
      if catch-all-node: block.call catch-all-node.value_

      one-level-node := node.children.get "+"
      if one-level-node:
        // Call recursively.
        do_ topic-segments (index + 1) one-level-node block

      node = node.children.get topic-segments[index]
      if not node: return
    if node.value_: block.call node.value_

  stringify [value-stringifier] -> string:
    result := ""
    root_.children.do: | key value |
      result += stringify_ value key 0 value-stringifier
    return result

  stringify_ node name indentation/int [value-stringifier] -> string:
    indentation-str := " " * indentation
    result := "$indentation-str$name"
    if node.value_: result += ": $(value-stringifier.call node.value_)"
    result += "\n"
    node.children.do: | key value |
      result += stringify_ value "$key" (indentation + 2) value-stringifier
    return result

class TopicTreeNode_:
  segment /string
  value_ /any := null
  children /Map ::= {:}  // string -> TopicTreeNode_?

  constructor .segment:
