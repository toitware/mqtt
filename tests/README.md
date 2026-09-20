# Tests

The tests use small protocol conversations instead of a callback-heavy broker
interception framework. `support/peer.toit` is a duplex byte pipe and a peer that
can send or receive one packet. A test chooses the response explicitly:

```toit
receipt := client.publish "sensor" #[1]
packet := peer.receive as PublishPacket
expect-not receipt.is-complete
peer.send (PubAckPacket --packet-id=packet.packet-id)
receipt.wait
```

Each named function describes a behavior. Files group behaviors by boundary:

- `wire-test`: fragmentation, truncation, invalid headers, byte limits, topics.
- `lifetime-test`: persistent completion, concurrent observers, fake timestamps.
- `client-test`: delivery, reconnection, session restoration, shutdown, rejection.
- `client-failures-test`: ACK during writes, uncertain writes, backoff cancellation,
  exhaustion, authentication, bounded inboxes, missing PINGRESP.
- `broker-test`: offline capacity, retained ownership, malformed clients, expiry,
  wills, connection/subscription limits, connection replacement.
- `file-store-test`: real files, cache eviction, corruption, failed writes, quotas,
  and background storage failure terminating the broker lifetime.
- `interop.toit`: the client against Mosquitto and Mosquitto clients against the broker.

Pure keepalive tests advance timestamps. One client test exercises the real ping
deadline through the scheduler; it waits for packets/results and does not sleep.
Protocol scenarios use bounded outer deadlines as deadlock guards. No test depends
on counting arbitrary scheduler yields to infer that a broker is idle.

Run `make test`, or `toit run tests/client-test.toit` for a focused scenario group.
Mosquitto tests are registered only when the broker and both CLI tools exist.
Host tests do not measure an ESP32 heap or validate an SD card driver.
