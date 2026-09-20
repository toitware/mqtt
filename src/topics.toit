// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/** Validates an MQTT UTF-8 string. */
validate-string value/string --allow-empty/bool=false -> none:
  if (not allow-empty and value.size == 0) or value.size > 0xffff or value.contains "\u0000":
    throw "INVALID_ARGUMENT"

/** Validates a publish topic. */
validate-topic topic/string -> none:
  validate-string topic
  if topic.contains "+" or topic.contains "#": throw "INVALID_ARGUMENT"

/** Validates a subscription filter. */
validate-filter filter/string -> none:
  validate-string filter
  levels := filter.split "/"
  levels.do: | level/string |
    if level.contains "+" and level != "+": throw "INVALID_ARGUMENT"
    if level.contains "#" and level != "#": throw "INVALID_ARGUMENT"
  levels.size.repeat: | index |
    if levels[index] == "#" and index != levels.size - 1: throw "INVALID_ARGUMENT"

/** Matches a validated filter against a validated topic. */
matches filter/string topic/string -> bool:
  if topic.starts-with "\$" and not filter.starts-with "\$": return false
  filters := filter.split "/"
  levels := topic.split "/"
  filters.size.repeat: | index |
    level := filters[index]
    if level == "#": return true
    if index >= levels.size: return false
    if level != "+" and level != levels[index]: return false
  return filters.size == levels.size
