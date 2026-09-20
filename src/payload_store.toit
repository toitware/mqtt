// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .errors

/**
Stores payloads independently of routing metadata.

The broker serializes access and owns every key. Put must publish a complete
  payload or throw; get must return owned bytes. A backend must not call back into
  the broker. Keys are scoped to one broker lifetime. Limits are enforced by the
  broker before put, regardless of whether the backend uses RAM or external storage.
*/
interface PayloadStore:
  put key/int bytes/ByteArray -> none
  get key/int -> ByteArray
  remove key/int -> none

/** Stores payloads in memory within the broker's configured budget. */
class MemoryPayloadStore implements PayloadStore:
  data_/Map := {:}
  put key/int bytes/ByteArray -> none:
    data_[key] = ByteArray.from bytes
  get key/int -> ByteArray:
    return ByteArray.from data_[key]
  remove key/int -> none:
    data_.remove key

/** A storage failure invalidates broker ownership and stops its lifetime. */
class StorageError extends MqttError:
  constructor cause:
    super "STORAGE_FAILED" --cause=cause

class PayloadPool_:
  store_/PayloadStore
  limit_/int
  bytes/int := 0
  entries_/Map := {:}
  next_/int := 1
  constructor .store_ .limit_:

  check size/int:
    if bytes + size > limit_: throw (CapacityError "stored payload bytes")

  add payload/ByteArray references/int -> int:
    check payload.size
    key := next_++
    failure := catch: store_.put key payload
    if failure: throw (StorageError failure)
    entries_[key] = [payload.size, references]
    bytes += payload.size
    return key

  retain key/int:
    entries_[key][1]++

  get key/int -> ByteArray:
    result/ByteArray? := null
    failure := catch: result = store_.get key
    if failure: throw (StorageError failure)
    if result.size != entries_[key][0]: throw (StorageError "payload length changed")
    return result

  release key/int:
    entry := entries_[key]
    if entry[1] > 1:
      entry[1]--
      return
    failure := catch: store_.remove key
    if failure: throw (StorageError failure)
    bytes -= entry[0]
    entries_.remove key

  close:
    failure/any := null
    entries_.keys.do: | key |
      error := catch: store_.remove key
      if error and not failure: failure = error
    entries_.clear
    bytes = 0
    if failure: throw (StorageError failure)
