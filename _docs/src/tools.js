// What the site says about each tool. README.md is the full reference; this is
// the version a visitor reads to learn how to call each one.
//
// params: [name, type, required, meaning]. Batch tools list their item fields,
// since the item is where everything that shapes the operation lives.

export const groups = {
  discover: 'discover',
  read: 'read',
  measure: 'measure',
  change: 'change',
}

export const tools = [
  {
    name: 'server_config',
    group: 'discover',
    summary: 'Which endpoint, cluster and policy this session reached, and which tools it has.',
    detail:
      'Call it first when a result is surprising. An empty topic list means one thing on a local broker and another on production. The password is never reported.',
    params: [],
    call: {},
    result: {
      endpoint: 'prod-read',
      path: '/mcp',
      cluster: 'prod',
      brokers: ['kafka-1:9093', 'kafka-2:9093'],
      authentication: 'SCRAM-SHA-256',
      sasl_user: 'kafka-mcp-readonly',
      tls: true,
      read_only: true,
      tools: ['compare_clusters', 'consumer_lag', 'copy_message', 'describe_topic', '…'],
    },
  },
  {
    name: 'list_clusters',
    group: 'discover',
    summary: 'Every cluster this server serves, whether it is reachable now, and whether it accepts writes.',
    detail:
      'Reachability is checked at call time. Only names are reported, never brokers or credentials, because every endpoint can call it.',
    params: [],
    call: {},
    result: {
      clusters: [
        { name: 'prod', connected: true, read_only: true },
        { name: 'preprod', connected: true, read_only: false },
      ],
      count: 2,
    },
  },
  {
    name: 'list_topics',
    group: 'discover',
    summary: 'Topics with partition count, replication factor and the configs each sets for itself.',
    detail:
      'The script is a JavaScript predicate over topic, partitions, replication_factor, internal and configs. A topic whose script throws is counted in script_errors, so a broken filter never looks like an empty cluster.',
    params: [
      ['script', 'string', false, 'Predicate deciding whether a topic is listed'],
      ['timeout_seconds', 'int', false, 'Limit for evaluating the script. Default 30'],
    ],
    call: { script: "return partitions > 6 && configs['cleanup.policy'] === 'compact'" },
    result: {
      topics: [{ topic: 'orders', partitions: 12, replication_factor: 3, configs: { 'cleanup.policy': 'compact' } }],
      count: 1,
    },
  },
  {
    name: 'describe_topic',
    group: 'discover',
    batch: 20,
    summary: 'Offset ranges, message count, time span and full configuration.',
    detail:
      'Use it before a search to see how much it would read, and how far back the topic can hold data at all: retention.ms and cleanup.policy decide whether a message can still exist.',
    params: [['topic', 'string', true, 'Topic to describe']],
    call: { items: [{ topic: 'orders' }, { topic: 'payments' }] },
    result: {
      results: [{ index: 0, result: { topic: 'orders', partition_count: 1, message_count: 3, partitions: [{ partition: 0, start_offset: 0, end_offset: 3 }] } }],
      succeeded: 2, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'compare_clusters',
    group: 'discover',
    batch: 100,
    summary: 'Topics only here, only there, or configured differently. Creates nothing.',
    detail:
      '"Here" is always the cluster this endpoint serves. Entries carry partitions, replication and explicit configs, so they can be passed straight to create_topic.',
    params: [
      ['cluster', 'string', true, 'The other cluster. See list_clusters'],
      ['search', 'string', false, 'Case-insensitive substring a topic name must contain'],
      ['include_internal', 'bool', false, 'Include internal topics. Default false'],
    ],
    call: { items: [{ cluster: 'prod', search: 'orders' }] },
    result: {
      here: 'preprod', there: 'prod',
      only_here: [{ topic: 'orders-v2', partitions: 6, replication_factor: 1 }],
      only_there: [],
      differing: [{ topic: 'orders', differences: ['partitions'], here: { partitions: 1 }, there: { partitions: 12 } }],
      in_both: 11,
    },
  },
  {
    name: 'sample_messages',
    group: 'read',
    batch: 20,
    summary: 'What recent messages look like: formats, field paths, schemas, and which field the key is.',
    detail:
      'Avro, Protobuf and JSON Schema values are decoded first, so their fields are listed like JSON. When key_in_value names a field, the key is that identifier, and searching the key is the exact, cheap lookup.',
    params: [
      ['topic', 'string', true, 'Topic to sample'],
      ['sample_size', 'int', false, 'Messages to read in total. Default 20'],
      ['partitions', 'int[]', false, 'Restrict to these partitions'],
      ['max_value_bytes', 'int', false, 'Value bytes per message. Default 512'],
    ],
    call: { items: [{ topic: 'orders', sample_size: 20 }] },
    result: {
      results: [{ index: 0, result: {
        value_formats: { json: 0, text: 0, binary: 0, avro: 20 },
        json_fields: [{ path: 'payload.amount', types: ['number'], present: 20 }],
        key_in_value: ['payload.orderId'],
        schemas: [{ format: 'avro', schema_id: 7, message_type: 'shop.Order', count: 20 }],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'search_messages',
    group: 'read',
    summary: 'A bounded scan filtered by JavaScript, reporting exactly what it covered.',
    detail:
      'The script sees value, key, headers, partition, offset and timestamp. An empty result means "not there" only when complete is true; otherwise read stopped_reason and narrow the search.',
    params: [
      ['topic', 'string', true, 'Topic to search'],
      ['script', 'string', false, 'Filter. Omit to match every message'],
      ['partitions', 'int[]', false, 'Restrict to these partitions'],
      ['from_offset / to_offset', 'int', false, 'Offset window, end exclusive'],
      ['from_timestamp / to_timestamp', 'string', false, 'RFC3339 time window'],
      ['direction', 'string', false, 'newest_first (default) or oldest_first'],
      ['max_matches', 'int', false, 'Stop after this many. Default 10'],
      ['max_messages_scanned', 'int', false, 'Read at most this many. Default 10000'],
      ['parallelism', 'int', false, 'Readers for one partition, 1 to 16. Default 1'],
      ['count_only', 'bool', false, 'Counts only, no bodies'],
      ['output_file', 'string', false, 'Write every match to this file as JSONL'],
      ['timeout_seconds', 'int', false, 'Wall-clock limit. Default 30'],
    ],
    call: { topic: 'orders', script: "return key === 'ORD-12345'", max_matches: 5 },
    result: {
      topic: 'orders', match_count: 1,
      matches: [{ partition: 3, offset: 48211, key: 'ORD-12345', value: '{"status":"NEW"…', encoding: 'utf8' }],
      scanned_messages: 120000, stopped_reason: 'range_exhausted', complete: true,
    },
  },
  {
    name: 'get_message',
    group: 'read',
    batch: 20,
    summary: 'Messages at exact offsets, with neighbours on either side.',
    detail:
      'Schema Registry values (Avro, Protobuf, JSON Schema) and configured formats come back decoded to JSON, with format and schema_id. Anything else that is not UTF-8 is base64; decode_error says why a schema-framed value could not be read.',
    params: [
      ['topic', 'string', true, 'Topic to read from'],
      ['partition', 'int', true, 'Partition to read from'],
      ['offset', 'int', true, 'Exact offset'],
      ['context', 'int', false, 'Also return this many messages either side'],
      ['max_value_bytes', 'int', false, 'Value bytes to return. Default 4096'],
    ],
    call: { items: [{ topic: 'orders', partition: 3, offset: 48211, context: 1 }] },
    result: {
      results: [{ index: 0, result: { topic: 'orders', message: { partition: 3, offset: 48211, key: 'ORD-12345', value: '{"status":"NEW",…}', format: 'avro', schema_id: 7 }, before: ['…'], after: ['…'] } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'get_schema',
    group: 'read',
    batch: 100,
    summary: 'A Schema Registry schema, by subject or by the schema id a message carries.',
    detail:
      'Read it before producing to a schema-encoded topic: it names every field a value needs. For Protobuf, message_types are the names produce_message accepts. Looking up an id lists the subjects that use it.',
    params: [
      ['subject', 'string', false, 'Usually <topic>-value. Give subject or id'],
      ['version', 'int', false, 'Defaults to the latest'],
      ['id', 'int', false, 'Schema id, e.g. from get_message'],
    ],
    call: { items: [{ subject: 'orders-value' }] },
    result: {
      results: [{ index: 0, result: {
        schema_id: 7, subject: 'orders-value', version: 3, versions: [1, 2, 3],
        type: 'avro', schema: '{"type":"record","name":"Order",…}', references: [],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'list_consumer_groups',
    group: 'measure',
    summary: 'Groups with their state, member count and the topics they consume.',
    detail: 'An Empty group can still report lag: committed offsets outlive the consumers that made them.',
    params: [
      ['topic', 'string', false, 'Only groups consuming or committed to this topic'],
      ['states', 'string[]', false, 'Filter by state, such as Stable or Empty'],
    ],
    call: { topic: 'orders', states: ['Stable'] },
    result: { groups: [{ group: 'payments', state: 'Stable', members: 2, topics: ['orders'] }], count: 1 },
  },
  {
    name: 'consumer_lag',
    group: 'measure',
    batch: 100,
    summary: 'Lag, produce and consume rates, and when the backlog clears, or that it never will.',
    detail:
      'status decides what the numbers mean: caught_up, draining, growing, stalled, no_active_consumers or not_measured. An ETA is only given when lag is really shrinking. The call blocks for sample_seconds.',
    params: [
      ['topic', 'string', true, 'Topic to measure'],
      ['group', 'string', false, 'Defaults to every group consuming the topic'],
      ['sample_seconds', 'int', false, 'Consume-rate sample window. Default 5'],
      ['skip_consume_rate', 'bool', false, 'Return at once, without rate or ETA'],
    ],
    call: { items: [{ topic: 'orders', group: 'payments' }] },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', total_lag: 4200,
        groups: [{ group: 'payments', lag: 4200, drain_per_second: 70, eta_human: '1m 0s', status: 'draining' }],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'describe_consumer_group',
    group: 'measure',
    batch: 100,
    summary: 'Members, who owns which partition, and the group\u2019s position and lag on each.',
    detail:
      'Turns a stuck partition into a pod: every partition names its member, client id and host. has_commit false means the group owns a partition it never committed on, so auto.offset.reset decides where it starts.',
    params: [['group', 'string', true, 'Consumer group to describe']],
    call: { items: [{ group: 'payments' }] },
    result: {
      results: [{ index: 0, result: {
        group: 'payments', state: 'Stable', assignor: 'cooperative-sticky', total_lag: 4200,
        members: [{ client_id: 'payments-7', host: '/10.0.4.17', assignments: [{ topic: 'orders', partitions: [0, 1] }] }],
        partitions: [{ topic: 'orders', partition: 0, committed_offset: 812, end_offset: 5012, lag: 4200, client_id: 'payments-7' }],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'open_transactions',
    group: 'measure',
    batch: 100,
    summary: 'Open transactions holding read_committed consumers back, and the producer holding them.',
    detail:
      'A hung transactional producer stalls every read_committed consumer at its first uncommitted message, which looks exactly like a poison message. The fix is restarting the producer named by transactional_id, not moving an offset.',
    params: [['topic', 'string', true, 'Topic to check']],
    call: { items: [{ topic: 'orders' }] },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', blocked: true,
        partitions: [{ partition: 0, last_stable_offset: 812, high_watermark: 5012, unreadable_messages: 4200,
          producers: [{ transactional_id: 'payments-writer-1', state: 'Ongoing', open_for: '41m12s', timeout_ms: 900000 }] }],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'cluster_health',
    group: 'measure',
    summary: 'Brokers, controller, and every offline, under-replicated or under-min-ISR partition.',
    detail:
      'under_min_isr is what makes acks=all producers fail with NOT_ENOUGH_REPLICAS. Brokers that do not report min.insync.replicas are listed in min_isr_unknown instead of being guessed at.',
    params: [
      ['search', 'string', false, 'Only topics containing this. Case-insensitive'],
      ['include_internal', 'bool', false, 'Also check __consumer_offsets and friends'],
    ],
    call: { search: 'orders' },
    result: {
      controller: 1, healthy: false,
      brokers: [{ id: 1, host: 'kafka-1', controller: true, leaders: 61 }],
      summary: { topics: 1, partitions: 12, offline: 0, under_replicated: 1, under_min_isr: 1 },
      problems: [{ topic: 'orders', partition: 4, issues: ['under_replicated', 'under_min_isr'], isr: [1], min_insync_replicas: 2 }],
    },
  },
  {
    name: 'list_acls',
    group: 'discover',
    summary: 'Access control entries, for when a client is refused with an authorization error.',
    detail:
      'A resource_name filter returns every ACL the broker applies to it, including prefixed and wildcard entries. A deny overrides any allow. SECURITY_DISABLED means the broker enforces no ACLs at all.',
    params: [
      ['principal', 'string', false, 'Such as User:payments'],
      ['resource_type', 'string', false, 'topic, group, cluster, transactional_id, delegation_token'],
      ['resource_name', 'string', false, 'Needs resource_type'],
    ],
    call: { principal: 'User:payments', resource_type: 'topic', resource_name: 'orders' },
    result: {
      acls: [{ principal: 'User:payments', host: '*', resource_type: 'topic', resource_name: 'orders',
        pattern_type: 'literal', operation: 'read', permission: 'allow' }],
      count: 1,
    },
  },
  {
    name: 'create_topic',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Creates topics. The preview is the broker\u2019s own validate-only answer.',
    detail:
      'Refuses a topic that already exists. Omitted counts use the broker default on Kafka 2.4+, and the real chosen numbers are read back after creation.',
    params: [
      ['topic', 'string', true, 'Name of the topic'],
      ['partitions', 'int', false, 'Can grow later, never shrink'],
      ['replication_factor', 'int', false, 'Cannot exceed the broker count'],
      ['configs', 'map', false, 'Topic config such as retention.ms'],
    ],
    call: { items: [{ topic: 'orders.dlq', partitions: 3, configs: { 'retention.ms': '1209600000' } }], confirm: false },
    result: {
      results: [{ index: 0, result: { topic: 'orders.dlq', partitions: 3, created: false, would_create: true } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'add_partitions',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Grows a topic. Irreversible: Kafka cannot reduce a partition count.',
    detail:
      'partitions is the final total, so repeating a call is safe. Keys move to new partitions and lose ordering, so a keyed topic also needs acknowledge_key_ordering.',
    params: [
      ['topic', 'string', true, 'Topic to change'],
      ['partitions', 'int', true, 'Final total, not the number to add'],
      ['acknowledge_key_ordering', 'bool', false, 'Required when messages are keyed'],
      ['sample_size', 'int', false, 'Messages inspected for keys. Default 20'],
    ],
    call: { items: [{ topic: 'orders', partitions: 12 }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', current_partitions: 6, requested_partitions: 12, would_apply: true,
        keyed_messages: true, consumer_groups: ['payments'], warnings: ['keys will move partitions…'],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'alter_topic_config',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Changes topic config incrementally, showing each key\u2019s current and requested value.',
    detail:
      'The broker validates the preview. Shortening retention.ms reports how many messages are already past the new limit; changing cleanup.policy is warned about. Keys not named keep their value.',
    params: [
      ['topic', 'string', true, 'Topic to change'],
      ['set', 'map', false, 'Keys to set, such as retention.ms'],
      ['delete', 'string[]', false, 'Overrides to remove, so the cluster default applies'],
    ],
    call: { items: [{ topic: 'orders', set: { 'retention.ms': '86400000' } }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', messages_past_retention: 18000, applied: false,
        changes: [{ key: 'retention.ms', current: '604800000', current_source: 'DYNAMIC_TOPIC_CONFIG', requested: '86400000' }],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'commit_offset',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Moves a group to an offset, a time or the earliest/latest position, to skip or to replay.',
    detail:
      'Give exactly one of offset, timestamp or position. With timestamp or position, omit partition to move the whole topic. The group must have no active members: a running consumer overwrites the commit. To skip offset 42, commit 43.',
    params: [
      ['topic', 'string', true, 'Topic whose offset moves'],
      ['group', 'string', true, 'Consumer group'],
      ['partition', 'int', false, 'Required with offset; omit otherwise for every partition'],
      ['offset', 'int', false, 'The offset the group reads next'],
      ['timestamp', 'string', false, 'RFC3339; first message at or after it'],
      ['position', 'string', false, 'earliest or latest'],
      ['allow_active_members', 'bool', false, 'Proceed despite running consumers'],
    ],
    call: { items: [{ topic: 'orders', group: 'payments', timestamp: '2026-10-01T09:00:00Z' }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', group: 'payments', state: 'Empty',
        partitions: [{ partition: 0, current_offset: 5012, target_offset: 4100, replayed_messages: 912 }],
        replayed_messages: 912, applied: false,
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'delete_topic',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Deletes topics, after showing the messages and groups it would destroy.',
    detail:
      'confirm says the caller meant it; acknowledge_data_loss says they know what is inside. Internal topics such as __consumer_offsets are refused at any level of consent.',
    params: [
      ['topic', 'string', true, 'Topic to delete. It must exist'],
      ['acknowledge_data_loss', 'bool', false, 'Required when the topic still holds messages'],
    ],
    call: { items: [{ topic: 'orders-old' }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders-old', partitions: 6, message_count: 41207, consumer_groups: ['payments'],
        deleted: false, would_delete: true,
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'delete_records',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Deletes a partition\u2019s oldest messages, keeping the topic and its groups.',
    detail:
      'Everything below before_offset goes. Needs confirm and acknowledge_data_loss. The preview names every group committed below the cut and how many messages it would lose unread.',
    params: [
      ['topic', 'string', true, 'Topic to delete from'],
      ['partition', 'int', true, 'Partition'],
      ['before_offset', 'int', true, 'This offset becomes the first readable one'],
      ['acknowledge_data_loss', 'bool', false, 'Required to apply'],
    ],
    call: { items: [{ topic: 'orders', partition: 0, before_offset: 812 }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', partition: 0, start_offset: 0, messages_deleted: 812, would_delete: true,
        affected_groups: [{ group: 'replay-job', committed_offset: 100, unprocessed_lost: 712 }],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'delete_consumer_group',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Deletes abandoned consumer groups and the lag they keep reporting.',
    detail:
      'Refuses a group with active members. The preview lists every committed offset and the lag that disappears. A consumer that reuses the id later starts from its auto.offset.reset.',
    params: [['group', 'string', true, 'Consumer group to delete']],
    call: { items: [{ group: 'old-billing' }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        group: 'old-billing', state: 'Empty', total_lag: 91234, would_delete: true, deleted: false,
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'copy_message',
    group: 'change',
    batch: 20,
    write: 'destination',
    summary: 'Copies a message by its address, to a dead letter topic or into another cluster.',
    detail:
      'Key, value and headers are preserved, and provenance headers record where it came from. A read-only endpoint can still be the source; only the destination must be writable. Across Schema Registries, translate_schema registers the schema at the destination and rewrites the id.',
    params: [
      ['source_topic', 'string', true, 'Message to copy'],
      ['source_partition', 'int', true, ''],
      ['source_offset', 'int', true, ''],
      ['destination_topic', 'string', true, 'Must already exist'],
      ['destination_cluster', 'string', false, 'Defaults to this endpoint\u2019s cluster'],
      ['translate_schema', 'bool', false, 'Re-register a schema id in the destination registry'],
      ['max_value_bytes', 'int', false, 'Preview limit. The whole value is always copied'],
    ],
    call: {
      items: [{ source_topic: 'orders', source_partition: 3, source_offset: 48211, destination_topic: 'orders', destination_cluster: 'preprod' }],
      confirm: false,
    },
    result: {
      results: [{ index: 0, result: {
        source_cluster: 'prod', destination_cluster: 'preprod', applied: false,
        provenance_headers: ['kafka-mcp-copied-from-cluster', 'kafka-mcp-copied-from-offset', '…'],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
  {
    name: 'produce_message',
    group: 'change',
    batch: 20,
    write: 'destination',
    summary: 'Writes a new message into an existing topic, marked so it is never mistaken for a real one.',
    detail:
      'Omit partition unless the exact partition is the point: the key decides placement, and a named partition breaks ordering for that key. For a schema-encoded topic give the value as JSON with value_schema; it is validated and encoded before anything is written. A produced message cannot be deleted.',
    params: [
      ['topic', 'string', true, 'Existing topic'],
      ['value', 'string', true, 'The message body'],
      ['key', 'string', false, 'Decides the partition'],
      ['headers', 'object', false, 'Header name to value'],
      ['partition', 'int', false, 'Exact partition'],
      ['encoding', 'string', false, 'utf8 (default) or base64'],
      ['value_schema', 'object', false, 'Encode value to a registry schema. {} = latest <topic>-value'],
      ['key_schema', 'object', false, 'Encode key to a registry schema. {} = latest <topic>-key'],
      ['destination_cluster', 'string', false, 'Defaults to this endpoint\u2019s cluster'],
    ],
    call: { items: [{ topic: 'orders', key: 'ORD-12345', value: '{"status":"NEW"}', value_schema: {} }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        destination_cluster: 'preprod', topic: 'orders', applied: false,
        value_encoding: { format: 'avro', schema_id: 7, subject: 'orders-value', version: 3 },
        provenance_headers: ['kafka-mcp-produced-at', 'kafka-mcp-produced-by-tool', '…'],
      } }],
      succeeded: 1, failed: 0, applied: 0, atomic: false,
    },
  },
]

export const scenarios = [
  {
    guide: 'find-message',
    ask: 'find the message for order 12345',
    flow: ['sample_messages', 'describe_topic', 'search_messages', 'get_message'],
    note: 'Samples first to learn whether the id is the key. A key search is exact; a body search also finds 1123.',
  },
  {
    guide: 'check-lag',
    ask: 'how far behind is payments?',
    flow: ['consumer_lag'],
    note: 'Drain rate is consume minus produce. An ETA appears only when lag is shrinking.',
  },
  {
    guide: 'skip-poison-message',
    ask: 'the consumer is stuck',
    flow: ['consumer_lag', 'open_transactions', 'describe_consumer_group', 'get_message', 'copy_message', 'commit_offset'],
    note: 'Rules out a hung transaction first, then keeps the bad message in a dead letter topic before moving past it.',
  },
  {
    guide: 'scale-partitions',
    ask: 'add partitions to orders',
    flow: ['consumer_lag', 'describe_topic', 'add_partitions'],
    note: 'Checks the group is growing, not stalled. Partitions do not help a consumer that is not consuming.',
  },
  {
    guide: 'create-topic',
    ask: 'we need a dead letter topic',
    flow: ['server_config', 'describe_topic', 'create_topic'],
    note: 'Partition count and retention chosen on purpose, validated by the broker first.',
  },
  {
    guide: 'produce-message',
    ask: 'reproduce this in preprod',
    flow: ['get_message', 'get_schema', 'list_clusters', 'copy_message', 'produce_message'],
    note: 'Takes a production message into preprod without writing a byte to production.',
  },
  {
    guide: 'compare-clusters',
    ask: 'what does preprod have that prod does not?',
    flow: ['compare_clusters', 'create_topic'],
    note: 'Reports what differs, never what is correct. A missing topic is often deliberate.',
  },
  {
    guide: 'delete-topic',
    ask: 'clean up these test topics',
    flow: ['server_config', 'delete_topic'],
    note: 'Shows message counts and affected groups first.',
  },
  {
    guide: 'replay-messages',
    ask: 'reprocess everything since 9 this morning',
    flow: ['server_config', 'describe_consumer_group', 'commit_offset'],
    note: 'One item moves every partition to the first message at that time, with the replay count shown first.',
  },
  {
    guide: 'tune-topic-config',
    ask: 'the disk is filling up, cut retention on orders',
    flow: ['server_config', 'describe_topic', 'alter_topic_config'],
    note: 'Shows how many messages the new retention makes deletable before anything changes.',
  },
  {
    guide: 'cluster-health',
    ask: 'producers fail with NOT_ENOUGH_REPLICAS',
    flow: ['cluster_health', 'describe_topic'],
    note: 'Names the partitions under min ISR and the broker that fell out of sync.',
  },
  {
    guide: 'purge-messages',
    ask: 'delete the test data but keep the topic',
    flow: ['server_config', 'describe_topic', 'delete_records', 'delete_consumer_group'],
    note: 'Truncates partitions and cleans up abandoned groups, naming every group that would lose unread messages.',
  },
  {
    guide: 'authorization-error',
    ask: 'payments gets TOPIC_AUTHORIZATION_FAILED',
    flow: ['server_config', 'list_acls'],
    note: 'Lists every ACL applied to the principal and resource, including prefixed and deny entries.',
  },
]
