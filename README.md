# mqtt

An MQTT v3.1.1 Toit client, with support for QoS 0 and 1.

## Example

This simple example shows how to publish and subscribe to an MQTT broker. You can find more examples in the [examples/](examples/) folder.

```
import mqtt
import net

main:
  socket := net.open.tcp_connect "127.0.0.1" 1883
  client := mqtt.Client
    "toit-client-id"
    mqtt.TcpTransport socket

  task::
    client.handle: | topic/string payload/ByteArray |
      print "Received message on topic '$topic': $payload"

  client.subscribe "device/events" --qos=1

  client.publish "device/data" "Hello World".to_byte_array
```
