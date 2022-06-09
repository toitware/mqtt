/**
A connection to the broker.

Primarily ensures that exceptions are handled correctly.
The first side (reading or writing) that detects an issue with the transport disconnects the transport.
  It throws the original exception. The other side simply throws a $CLIENT_CLOSED_EXCEPTION.

Also sends ping requests to keep the connection alive (if $keep_alive is called).
*/
class Connection_:
  transport_ / ActivityMonitoringTransport

  /** Constructs a new connection. */
  constructor .transport_ --keep_alive/Duration?:
    throw "UNIMPLEMENTED"

  is_alive -> bool: throw "UNIMPLEMENTED"
  is_closed -> bool: throw "UNIMPLEMENTED"

  /**
  Starts a task to keep this connection to the broker alive.
  Sends ping requests when necessary.
  */
  keep_alive --background/bool:
    throw "UNIMPLEMENTED"

  /**
  Receives an incoming packet.
  */
  read -> Packet?:
    throw "UNIMPLEMENTED"

  /** Whether the connection is in the process of writing. */
  is_writing -> bool:
    throw "UNIMPLEMENTED"

  /**
  Writes the given $packet, serializing it first.
  */
  write packet/Packet:
    throw "UNIMPLEMENTED"

  /**
  Closes the connection.

  If $reason is not null, the closing happens due to an error.
  Closes the underlying transport.
  */
  close --reason=null:
    throw "UNIMPLEMENTED"


/**
A strategy to connect to the broker.

This class deals with (temporary) disconnections and timeouts.
It keeps track of whether trying to connect again makes sense, and when it
  should try again.

It is also called for the first time the client connects to the broker.
*/
interface ReconnectionStrategy:
  /**
  Is called when the client wants to establish a connection through the given $transport.

  The strategy should first call $send_connect, followed by a $receive_connect_ack. If
    the connection is unsuccessful, it may retry.

  The $receive_connect_ack block returns whether the broker had a session for this client.

  The $is_initial_connection is true if this is the first time the client connects to the broker.
  */
  connect -> none
      transport/ActivityMonitoringTransport
      --is_initial_connection /bool
      [--reconnect_transport]
      [--send_connect]
      [--receive_connect_ack]
      [--disconnect]

  /** Whether the client should even try to reconnect. */
  should_try_reconnect transport/ActivityMonitoringTransport -> bool

  /** Whether the strategy is closed. */
  is_closed -> bool

  /** Closes the strategy, indicating that no further reconnection attempts should be done. */
  close -> none

/**
A base class for reconnection strategies.
*/
abstract class DefaultReconnectionStrategyBase implements ReconnectionStrategy:
  UNIMPLEMENTED ::= "UNIMPLEMENTED"

/**
An MQTT session.

Keeps track of data that must survive disconnects (assuming the user didn't use
  the 'clean_session' flag).
*/
class Session_:
  next_packet_id_ /int? := 1

  /** Packets that are still missing an ack. */
  pending_ /Map ::= {:}  // From packet_id to persistent_id.

  options /SessionOptions

  constructor .options:

  set_pending_ack --packet_id/int --persistent_id/int:
    pending_[packet_id] = persistent_id

  handle_ack id/int [--if_absent] -> int?:
    result := pending_.get id
    pending_.remove id --if_absent=if_absent
    return result

  remove_pending id/int -> none:
    pending_.remove id

  next_packet_id -> int:
    return next_packet_id_++

  /**
  Runs over the pending packets.

  It is not allowed to change the pending map while calling this method.

  Calls the $block with the packet_id and persistent_id of each pending packet.
  */
  do --pending/bool [block] -> none:
    pending_.do block

/**
A persistence strategy for the MQTT client.

Note that the client does not automatically resend old messages when it starts up.
This is also true if the client tries to connect reusing an existing session, but
  the session has expired. In these cases, the user must manually resend the old
  messages.
*/
interface PersistenceStore:
  /**
  Stores the publish-packet information in the persistent store.
  Returns a persistent-store id which can be used to $get or $remove the data
    from the store.
  */
  store topic/string payload/ByteArray --retain/bool -> int

  /**
  Finds the persistent packet with $persistent_id and calls the given $block
    with arguments topic, payload and retain (in that order).

  The store may decide not to resend a packet, in which case it calls
    $if_absent with the $persistent_id.
  */
  get persistent_id/int [block] [--if_absent] -> none

  /**
  Removes the data for the packet with $persistent_id.
  Does nothing if there is no data associated to $persistent_id.
  */
  remove persistent_id/int -> none

  /**
  Calls the given block for each stored packet.

  The arguments to the block are:
  - the persistent id
  - the topic
  - the payload
  - the retain flag
  */
  do [block] -> none

/**
A persistence store that stores the packets in memory.
*/
class MemoryPersistenceStore implements PersistenceStore:
  UNIMPLEMENTED ::= "UNIMPLEMENTED"
