# mqtt

A MQTT v3.1.1 Client, with support for Qos 0 and 1.

## Example

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
