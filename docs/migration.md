# Migrating to the owned client

The three old clients are replaced by `mqtt.Client`. The minimum SDK is now
v2.0.0-alpha.198, the version used for the local validation.

| Previous API | New API |
| --- | --- |
| `SimpleClient`, routing `Client`, `FullClient` | `Client --connector --options` |
| Already-open/reconnecting `Transport` | `Connector.open` creates a fresh owned `Link` |
| `TcpTransport --host` | `TcpConnector --host` |
| `TcpTransport.tls` | `TcpConnector --secure --port=8883` |
| `start --client-id` or `connect` plus `handle` | Construct `SessionOptions`, then `start` |
| `publish` success had different meanings | Receipt: QoS 1 PUBACK; QoS 0 completed write |
| `subscribe topic callback` | `(subscribe filter).wait`, then `receive` in application code |
| `on-error` | Persistent `wait-closed` result, failed receipts, and throwing `receive` |
| `ReconnectionStrategy` and connection callbacks | `RetryPolicy` decides a delay for a classified failure |
| `max-inflight` | `SessionOptions --max-pending`, plus client byte limits |
| Streamed packet payload in a handler | Complete bounded `Message` owned by the receiver |
| Client persistence hooks | Bounded in-memory pending operations for one client lifetime |
| Unbounded test broker | `mqtt.broker.Broker` with `BrokerLimits` and `PayloadStore` |

The old client persistence interface is deliberately removed. Neither client nor
broker currently recovers MQTT session metadata across process restart. QoS 1
reconnection within a client lifetime is supported; broker payload spill is a
separate storage concern. Applications requiring reboot durability must keep their
own application outbox until a receipt succeeds.

Process received messages in an application task. Route using your application's
logic or `mqtt.topics.matches`; overlapping subscriptions can produce duplicates
with some external brokers, so application logic should tolerate QoS 1 duplicates.
Incoming QoS 1 messages are acknowledged after the client accepts the complete
message into its bounded inbox, before the application processes it. This is not
an application-processing acknowledgement. If the inbox is full, the client
fails visibly rather than dropping an already acknowledged message.

A terminal error is persistent. Multiple callers can observe the same error object;
no caller consumes it. Cleanup and waiter completion never depend on a user error
callback. A callback you invoke while consuming `receive` is your task's exception;
use `try/finally` to close the client when that task ends.
