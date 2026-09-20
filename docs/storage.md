# Broker storage and memory budgets

The broker owns routing metadata. `PayloadStore` owns payload bytes, identified by
integer keys. One payload can have several references: retained data, queued
subscriber deliveries, and an owner's will. The broker releases the key only when
the last reference is gone. It checks every recipient's capacity before admitting
a publish and acknowledges QoS 1 only after admission succeeds.

`BrokerLimits` bounds active connections, persistent sessions, filters and filter
bytes per session, deliveries per session, total deliveries, retained topics,
stored payload bytes, encoded packet bytes, topic bytes, and client ID bytes.
Zero-length payloads still consume delivery/retained/session slots. There is no
unbounded offline queue. At capacity, subscriptions receive a failed SUBACK, new
sessions are refused, or a publisher is disconnected without PUBACK. Wills that
cannot be routed within capacity increment `stats["dropped-wills"]`.

These are logical budgets, not a claim that the process fits in exactly
`payload-bytes`. RAM also includes bounded maps/strings, TCP/TLS buffers, one
packet being read and written per active connection, and temporary codec copies.
Start with the default four connections, 4 KiB packets, and 32 KiB payload store;
measure the complete application on the target ESP32 and lower the limits as needed.
Disconnected sessions expire lazily on the next connection or when
`expire-sessions` is called. A full table never silently evicts unexpired sessions.

For SD-backed storage, implement the four methods of `PayloadFiles` using the
chosen filesystem, scoped to a fresh directory owned by the broker. Construct:

```toit
files := FilePayloadStore sd-files
store := CachedPayloadStore files --ram-bytes=4096
broker := Broker listener --store=store
    --limits=(BrokerLimits --payload-bytes=1_048_576)
```

`FilePayloadStore` writes a temporary file and renames it after the write completes.
It retains only size and SHA-256 metadata in RAM. Reads are bounded by the recorded
size and checked against the hash. The optional cache keeps recently used bytes
within `ram-bytes`; evicting cache entries leaves their authoritative files intact.
With `ram-bytes=0`, payloads are loaded only when needed. The broker serializes
storage access; adapters must not call back into MQTT.

Storage failure terminates the broker lifetime and wakes its waiters, because
continuing with uncertain payload ownership could silently lose acknowledged data.
The real-filesystem tests cover eviction/readback, failed rename, corruption,
quota enforcement, and cleanup. No particular SD hardware/filesystem is required
by the MQTT package.

This is a spill store for one running broker. It does not persist subscriptions,
packet IDs, session metadata, or delivery transactions across power loss. Give each
broker lifetime an empty directory; after a crash the application may reclaim its
old directory. Surviving reboot would require a journal for metadata and a defined
filesystem flush/durability contract, in addition to these payload files.
