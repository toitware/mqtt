// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A topic for subscribing to MQTT topics.
*/
class TopicQos:
  topic   /string
  max-qos /int

  /**
  Constructs a subscription topic with its max qos.

  The chosen $max-qos is the maximum QoS the client will receive. The broker
    generally sends a packet to subscribers with the same QoS as the one it
    received it with. The $max-qos parameter sets a limit on which QoS the client
    wants to receive.
  */
  constructor .topic --.max-qos=1:
