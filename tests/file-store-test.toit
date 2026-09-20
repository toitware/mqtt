// Copyright (C) 2026 Toit contributors.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import host.file
import host.directory
import mqtt.file-payload-store show *
import mqtt.payload-store show *
import mqtt.packets show *
import mqtt.topic-qos show *
import mqtt.bounded-broker show Broker BrokerLimits
import .support.peer

/** A real filesystem adapter, using a fresh test-owned directory. */
class DiskFiles implements PayloadFiles:
  directory/string
  fail-rename := false
  reads := 0
  constructor .directory:
  write name/string bytes/ByteArray -> none:
    file.write-contents bytes --path="$directory/$name"
  read name/string --max-size/int -> ByteArray:
    reads++
    path := "$directory/$name"
    stream := file.Stream.for-read path
    try:
      bytes := stream.in.read-bytes max-size
      if stream.in.try-ensure-buffered 1: throw "OVERSIZED_FILE"
      return bytes
    finally:
      stream.close
  rename from/string to/string -> none:
    if fail-rename: throw "DISK_FAILED"
    file.rename "$directory/$from" "$directory/$to"
  remove name/string -> none:
    path := "$directory/$name"
    if file.is-file path: file.delete path

class FailingReads extends MemoryPayloadStore:
  get key/int -> ByteArray:
    throw "SD_REMOVED"

background-storage-failure-stops-broker:
  listener := MemoryListener
  broker := Broker listener --store=FailingReads
  broker.start
  broker.publish "a" #[1] --retain
  peer := Peer
  listener.add peer.client-link
  peer.send (ConnectPacket "reader" --clean-session=true --username=null --password=null
      --keep-alive=Duration.ZERO
      --last-will=null)
  expect peer.receive is ConnAckPacket
  peer.send (SubscribePacket [TopicQos "a"] --packet-id=1)
  failure := catch: broker.wait-closed
  expect failure is StorageError
  expect-equals "SD_REMOVED" failure.cause
  expect-identical failure (catch: broker.wait-closed)

main:
  with-timeout --ms=5_000: background-storage-failure-stops-broker
  path := directory.mkdtemp "/tmp/mqtt-store-"
  try:
    files := DiskFiles path
    disk := FilePayloadStore files
    store := CachedPayloadStore disk --ram-bytes=3
    store.put 1 #[1, 2, 3]
    store.put 2 #[4, 5, 6]
    expect-equals 3 store.cached-bytes
    expect-equals #[1, 2, 3] (store.get 1)
    expect-equals 1 files.reads
    expect-equals #[1, 2, 3] (store.get 1)
    expect-equals 1 files.reads
    store.remove 1
    store.remove 2
    expect-equals 0 store.cached-bytes

    // A failed publication leaves no complete or temporary payload file.
    files.fail-rename = true
    expect-not-null (catch: disk.put 3 #[1])
    expect-not (file.is-file "$path/3.pending")
    expect-not (file.is-file "$path/3.payload")
    files.fail-rename = false

    // Detect corruption, including corrupt length before reading into memory.
    disk.put 4 #[1, 2]
    files.write "4.payload" #[9, 9]
    expect-not-null (catch: disk.get 4)
    files.write "4.payload" #[1, 2, 3]
    expect-not-null (catch: disk.get 4)
    disk.remove 4

    // Broker admission is bounded even when the payload lives on disk.
    broker := Broker MemoryListener --store=(CachedPayloadStore disk --ram-bytes=0)
        --limits=(BrokerLimits --payload-bytes=3)
    broker.start
    broker.publish "retained" #[1, 2, 3] --retain
    expect-equals 3 broker.stats["payload-bytes"]
    expect-not-null (catch: broker.publish "other" #[4] --retain)
    broker.close
    broker.wait-closed
    expect-not (file.is-file "$path/1.payload")
  finally:
    directory.rmdir path --recursive
