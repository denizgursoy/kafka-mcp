# Skip message

Use the Kafka MCP tools to investigate problematic Kafka messages.

## Finding a problematic message

When the user asks to find a bad or problematic Kafka message:

1. Check that the topic exists using `list_topics`.
2. Retrieve topic metadata using `get_topic_metadata`.
3. Prefer targeted searches using:
    - message key
    - transaction ID
    - correlation ID
    - partition
    - time range
4. Avoid scanning the entire topic unless necessary.
5. Retrieve candidate messages with `get_message`.
6. Validate the message using `validate_message`.
7. Explain why the message is invalid.


## Skipping a message

If a consumer is blocked by a bad message:

1. Identify the consumer group.
2. Check the current partition and offset.
3. Preserve the original message in the DLQ when possible.
4. Require explicit approval before changing offsets.
5. Advance the consumer past the problematic offset.
6. Verify that consumption continues.