# mqtt

An MQTT 3.1.1 client and bounded embedded broker for Toit, supporting QoS 0 and 1.
Requires Toit v2.0.0-alpha.198 or newer. This is a breaking API redesign;
see [migration](docs/migration.md).

## Client

```toit
import mqtt

main:
  client := mqtt.Client --connector=(mqtt.TcpConnector --host="localhost")
      --options=(mqtt.SessionOptions --client-id="sensor" --clean-session)
  client.start
  try:
    with-timeout --ms=30_000:
      client.wait-connected
      delivery := client.publish "sensor/temperature" "21.5"
      delivery.wait
  finally:
    client.close --force
    client.wait-closed
```

`publish`, `subscribe`, and `unsubscribe` return receipts. Admission is bounded by
operation count and bytes. A QoS 1 publish completes on PUBACK; QoS 0 completes
after writing. A subscription returns its granted QoS or throws on rejection.
Payloads are copied when admitted. An accepted QoS 1 operation remains pending
across reconnection, including an uncertain write, and may be delivered more than
once. Timing out on `receipt.wait` does not cancel delivery.

`start` runs the protocol owner in the background. `wait-closed` observes the
client's lifetime: normal close returns, terminal failure throws its stored cause.
All pending operations observe terminal failure too. `receive` drains complete
accepted messages, then returns null for normal closure or throws on failure.
It is safe to publish while processing an incoming message; application handlers
never run on the protocol reader. Keep a foreground task receiving, waiting on
receipts, or waiting on the lifetime while the client is needed.

`connection-state` and `last-connection-error` expose recovery diagnostics without
consuming failures or running callbacks. Transient connection failures retry with capped exponential delays. A stable
connection resets the delay history. Protocol errors and authentication refusal
are terminal; a broker's SERVER_UNAVAILABLE response can retry. Configure
`RetryPolicy --attempts=0` to disable retries. `wait-connected` waits for an online
connection but cannot promise the socket will remain alive afterwards.

The owner restores desired subscriptions when the broker loses its session and
replays pending QoS 1 messages. Keepalive zero disables pings; otherwise a missing
PINGRESP closes the attempt. There is no implicit PUBACK delivery deadline:
applications can bound receipt waits and explicitly close if their delivery policy
requires abandoning the client. `close` requests bounded graceful shutdown;
`close --force` aborts the attempt. Both wake blocked admission calls. Neither
promises to flush all unacknowledged messages before closing.

For TLS use `TcpConnector --secure --port=8883`, supplying root certificates and,
when needed, a server name or client certificate. A custom `Connector` returns a
fresh `Link` on every attempt. Links never reopen and must unblock I/O on close.

## Embedded broker

```toit
import net
import mqtt
import mqtt.broker as server

main:
  network := net.open
  broker := server.Broker (mqtt.TcpListener (network.tcp-listen 1883))
      --limits=(server.BrokerLimits --connections=4 --payload-bytes=32_768)
  try:
    broker.start
    broker.wait-closed
  finally:
    broker.close
    broker.wait-closed
    network.close
```

The broker bounds connections, persistent sessions, subscriptions, queues, retained
messages, wills, and payload bytes. It disconnects a publisher without PUBACK when
it cannot accept its message. It supports an optional authentication predicate,
per-connection handshake/write/idle timeouts, and session expiry. Invalid and slow
clients are isolated. Storage failure terminates the broker lifetime. `stats`
reports resource usage, client failures, and wills dropped because capacity was
exhausted.

See [storage and memory budgets](docs/storage.md) for the RAM cache and file spill
store, including the filesystem interface for SD storage. Payload spill survives
network disconnects during a running broker; MQTT session recovery after power
loss is not implemented. Hardware-specific SD setup and ESP32 memory measurement
remain application integration work.

## Development

[Ownership](docs/ownership.md) explains the architecture and failure boundaries.
[Tests](tests/README.md) explains the short, scripted protocol scenarios.

Run `make test`. The deterministic tests need no broker. If `mosquitto`,
`mosquitto_pub`, and `mosquitto_sub` are installed, CMake also enables an
interoperability test. Disable it with `cmake -B build -DWITH_MOSQUITTO=OFF`.
