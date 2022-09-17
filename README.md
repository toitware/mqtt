# mqtt

An MQTT v3.1.1 Toit client, with support for QoS 0 and 1.

## Example

This simple example shows how to publish and subscribe to an MQTT broker. You can find more examples in the [examples/](examples/) folder.

```
import mqtt
import net

main:
  transport := mqtt.TcpTransport net.open --host="127.0.0.1"
  client := mqtt.Client --transport=transport

  client.start --client_id="toit-client-id"

  client.subscribe "device/events":: | topic/string payload/ByteArray |
    print "Received: $topic: $payload.to_string_non_throwing"

  client.publish "device/data" "Hello, world!".to_byte_array
```
