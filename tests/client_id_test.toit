        // We are not using the codes from the packet file, just to have a bit of
        // redundancy. We want to avoid bad copy/pasting.

        if client_id == "REFUSED-UNACCEPTABLE":
          logger_.info "refusing connection request because of unnacceptable protocol version"
          // We are not using the codes from the packet file, just to have a bit of
          // redundancy. We want to avoid bad copy/pasting.
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x01
        else if client_id == "REFUSED-IDENTIFIER-REJECTED":
          logger_.info "refusing connection request because of rejected identifier"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x02
        else if client_id == "REFUSED-SERVER-UNAVAILABLE":
          logger_.info "refusing connection request because of unavailable server"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x03
        else if client_id == "REFUSED-BAD-USERNAME-PASSWORD":
          logger_.info "refusing connection request because of bad username/password"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x04
        else if client_id == "REFUSED-NOT-AUTHORIZED":
          logger_.info "refusing connection request because of not authorized"
          connack = mqtt.ConnAckPacket --session_present=false --return_code=0x05
        else:
