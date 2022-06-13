// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import monitor  // For toitdoc.

/**
A copy of $monitor.Latch.

Older version of the SDK don't have the $has_value function, so
  we use a copy here.
*/
// TODO(florian): remove copy of the Latch and use the original one
// from the monitor library.
monitor Latch:
  has_value_ := false
  value_ := null

  get:
    await: has_value_
    return value_

  set value:
    value_ = value
    has_value_ = true

  has_value -> bool:
    return has_value_
