# MQTT ownership

This stack replaces the legacy clients with a client that has one lifetime owner.
The wire codec is the first boundary: a decoded packet owns its complete payload;
no transport read is hidden in an application callback. It rejects oversized frames
before allocating the body and validates MQTT 3.1.1 QoS 0/1 packets.

The following layers will add bounded admission, observable operation completion,
a reconnecting lifetime, and a broker whose memory use is explicitly limited.
