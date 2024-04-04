// Copyright (C) 2024 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io

io-data-to-byte-array_ data/io.Data from/int=0 to/int=data.byte-size -> ByteArray:
  if data is ByteArray:
    byte-array := data as ByteArray
    if from == 0 and to == byte-array.size: return byte-array
    return byte-array[from..to]

  byte-array := ByteArray (to - from)
  data.write-to-byte-array byte-array --at=0 from to
  return byte-array
