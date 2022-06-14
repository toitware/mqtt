// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import monitor // For toitdoc.
import writer // For toitdoc.

/*
A (trimmed down) copy of $writer.Writer.

Older version of the SDK require the `writer_` to have `write data from to`.
That doesn't work with our Transport class, so we use a copy of the newer
  version here.
*/
// TODO(florian): remove copy of the Writer.
class Writer:
  writer_ := ?

  constructor .writer_:

  write data/ByteArray:
    size := data.size
    written := 0
    while written < size:
      written += writer_.write data[written..]
      if written != size: yield
    return size

  close -> none:
    writer_.close

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

