// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.


/**
A topic filter for subscribing to MQTT topics.
*/
class TopicFilter:
  filter/string
  max_qos/int

  /**
  Constructs a topic filter.

  The chosen $max_qos is the maximum QoS the client will receive. The broker
    generally sends a packet to subscribers with the same QoS as the one it
    received it with. The $max_qos parameter sets a limit on which QoS the client
    wants to receive.
  */
  constructor .filter --.max_qos=1:
