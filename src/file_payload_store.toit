// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import crypto.sha256 show sha256
import .payload-store

/**
A filesystem adapter confined to a directory owned by one broker lifetime.

Write closes its file before returning. Rename atomically publishes a completed
  file. Read must enforce max-size before allocating. Remove is idempotent.
  Implement this with the chosen SD filesystem or a host filesystem.
*/
interface PayloadFiles:
  write name/string bytes/ByteArray -> none
  read name/string --max-size/int -> ByteArray
  rename from/string to/string -> none
  remove name/string -> none

/**
Offloads payload bytes to files; keeps lengths and hashes in bounded broker metadata.

Use an empty, exclusively owned directory for each broker lifetime. This store is
  a spill area, not recovery of MQTT sessions after reboot. After a crash the
  application may delete its old spill directory before starting a fresh broker.
*/
class FilePayloadStore implements PayloadStore:
  files_/PayloadFiles
  entries_/Map := {:}
  constructor .files_:

  put key/int bytes/ByteArray -> none:
    if entries_.contains key: throw "DUPLICATE_PAYLOAD_KEY"
    pending := "$(key).pending"
    name := "$(key).payload"
    success := false
    try:
      digest := sha256 bytes
      files_.write pending bytes
      files_.rename pending name
      entries_[key] = [bytes.size, digest]
      success = true
    finally:
      if not success: catch: files_.remove pending

  get key/int -> ByteArray:
    entry := entries_[key]
    bytes := files_.read "$(key).payload" --max-size=entry[0]
    if bytes.size != entry[0] or (sha256 bytes) != entry[1]: throw "CORRUPT_PAYLOAD"
    return bytes

  remove key/int -> none:
    files_.remove "$(key).payload"
    entries_.remove key

/**
Adds a byte-bounded least-recently-used RAM cache over an authoritative backing store.

Writes go to the backing store before they are accepted. Cache eviction never
  drops delivery ownership; unused payloads are read back on demand. A zero-byte
  cache retains no payload bytes in RAM between operations.
*/
class CachedPayloadStore implements PayloadStore:
  backing_/PayloadStore
  budget_/int
  cache_/Map := {:}
  cached-bytes/int := 0
  constructor .backing_ --ram-bytes/int=4096:
    if ram-bytes < 0: throw "INVALID_ARGUMENT"
    budget_ = ram-bytes

  put key/int bytes/ByteArray -> none:
    backing_.put key bytes
    cache_ key bytes

  get key/int -> ByteArray:
    cached/ByteArray? := cache_.get key
    if cached:
      cache_.remove key
      cache_[key] = cached
      return ByteArray.from cached
    bytes := backing_.get key
    cache_ key bytes
    return bytes

  remove key/int -> none:
    backing_.remove key
    if cache_.contains key:
      cached-bytes -= cache_[key].size
      cache_.remove key

  cache_ key/int bytes/ByteArray:
    if cache_.contains key:
      cached-bytes -= cache_[key].size
      cache_.remove key
    if bytes.size > budget_ or budget_ == 0: return
    while not cache_.is-empty and cached-bytes + bytes.size > budget_:
      oldest := cache_.keys.first
      cached-bytes -= cache_[oldest].size
      cache_.remove oldest
    cache_[key] = ByteArray.from bytes
    cached-bytes += bytes.size
