#!/bin/bash

# Check if the phone number is provided as an argument
if [ -z "$1" ]; then
  echo "Error: No phone number provided."
  echo "Usage: $0 <phone_number>"
  exit 1
fi

# Assign the phone number to a variable
PHONE=$1

# Publish the message to NATS
nats publish processor.transformer.composite --count 1 \
'{
  "type": "query_state_entry",
  "route_id": "27bce142-8713-413a-930b-fc2783bab872:7c2ea117-b281-4b36-add9-e582d1a14fc2",
  "query_state": [
    {
      "__composite_key__": "5555",
      "first_name": "kasra",
      "last_name": "rasaee",
      "contact": "'$PHONE'"
    }
  ]
}' \
--server nats://127.0.0.1

