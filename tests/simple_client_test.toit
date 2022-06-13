// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import net

import .broker_internal
import .broker_mosquitto
import .transport

test_pubsub create_transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create_transport.call
  client := mqtt.Client --transport=transport --logger=logger
  options := mqtt.SessionOptions --client_id="test_pubsub"
  client.start --options=options

  done := monitor.Latch

  foo_sharp_counter := 0
  longer_topic_counter := 0
  client.subscribe "foo/#":: foo_sharp_counter++
  client.subscribe "foo/bar/gee":: longer_topic_counter++
  client.subscribe "done":: done.set true

  client.publish "foo/non_bar" "msg".to_byte_array --qos=0
  client.publish "foo/non_bar2" "msg".to_byte_array --qos=1

  client.publish "foo/bar/gee" "msg".to_byte_array --qos=0
  client.publish "foo/bar/gee" "msg2".to_byte_array --qos=1
  client.publish "foo/bar/gee" "msg3".to_byte_array --qos=1

  client.publish "done" "done".to_byte_array --qos=0
  done.get

  expect_equals 2 foo_sharp_counter
  expect_equals 3 longer_topic_counter

  client.unsubscribe "foo/bar/gee"
  client.publish "foo/bar/gee" "msg4".to_byte_array --qos=1

  done = monitor.Latch
  client.publish "done" "done".to_byte_array --qos=0
  done.get

  expect_equals 3 longer_topic_counter

  client.close

test_unclean_session create_transport/Lambda --logger/log.Logger:
  other_transport /mqtt.Transport := create_transport.call
  other_client := mqtt.Client --transport=other_transport --logger=logger
  other_client.start --client_id="other"

  unclean_transport /mqtt.Transport := create_transport.call
  unclean_client := mqtt.Client --transport=unclean_transport --logger=logger
  unclean_client.start --client_id="unclean"

  done := monitor.Latch
  unclean_message_counter := 0
  unclean_client.subscribe "foo/#":: unclean_message_counter++
  unclean_client.subscribe "done":: done.set true

  unclean_client.publish "done" "done".to_byte_array --qos=0
  done.get
  unclean_client.close

  // While the client is away, send some messages to it.
  other_client.publish "foo/bar" "msg".to_byte_array --qos=1
  other_client.publish "foo/gee" "msg".to_byte_array --qos=1

  // Connect again.
  // Since we don't give routes to the client, the catch-all-callback will handle the messages.
  unclean_transport = create_transport.call
  unclean_client = mqtt.Client --transport=unclean_transport --logger=logger
  catch_all_counter := 0
  unclean_client.start --client_id="unclean"
      --catch_all_callback=:: | topic/string payload/ByteArray |
        catch_all_counter++

  done = monitor.Latch
  unclean_client.subscribe "done":: done.set true
  unclean_client.publish "done" "done".to_byte_array --qos=0
  done.get

  expect_equals 2 catch_all_counter
  unclean_client.close

  // Send more messages.
  // The client is again not connected.
  other_client.publish "foo/handled1" "msg".to_byte_array --qos=1
  other_client.publish "foo/handled2" "msg".to_byte_array --qos=1

  unclean_transport = create_transport.call
  routed_messages_counter := 0
  unclean_client = mqtt.Client --transport=unclean_transport --logger=logger --routes={
    "foo/#": :: | topic/string payload/ByteArray |
      expect (topic.starts_with "foo/handled")
      routed_messages_counter++
      expect_equals "msg" payload.to_string
  }
  catch_all_counter = 0
  unclean_client.start --client_id="unclean"
      --catch_all_callback=:: | topic/string payload/ByteArray |
        catch_all_counter++

  done = monitor.Latch
  unclean_client.subscribe "done":: done.set true
  unclean_client.publish "done" "done".to_byte_array --qos=0
  done.get

  expect_equals 0 catch_all_counter
  expect_equals 2 routed_messages_counter

  unclean_client.close
  other_client.close

test create_transport/Lambda --logger/log.Logger:
  test_pubsub create_transport --logger=logger
  test_unclean_session create_transport --logger=logger

main args:
  test_with_mosquitto := args.contains "--mosquitto"
  log_level := log.ERROR_LEVEL
  logger := log.default.with_level log_level

  run_test := : | create_transport/Lambda | test create_transport --logger=logger
  with_internal_broker --logger=logger run_test
  if test_with_mosquitto: with_mosquitto --logger=logger run_test
