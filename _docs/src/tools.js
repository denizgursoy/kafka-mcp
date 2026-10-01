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
    summary: 'What recent messages look like: formats, JSON field paths, and which field the key is.',
    detail:
      'When key_in_value names a field, the key is that identifier, and searching the key is the exact, cheap lookup.',
    params: [
      ['topic', 'string', true, 'Topic to sample'],
      ['sample_size', 'int', false, 'Messages to read in total. Default 20'],
      ['partitions', 'int[]', false, 'Restrict to these partitions'],
      ['max_value_bytes', 'int', false, 'Value bytes per message. Default 512'],
    ],
    call: { items: [{ topic: 'orders', sample_size: 20 }] },
    result: {
      results: [{ index: 0, result: {
        value_formats: { json: 20, text: 0, binary: 0 },
        json_fields: [{ path: 'payload.amount', types: ['number'], present: 20 }],
        key_in_value: ['payload.orderId'],
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
    detail: 'Values that are not valid UTF-8 come back base64 encoded, with encoding set to base64.',
    params: [
      ['topic', 'string', true, 'Topic to read from'],
      ['partition', 'int', true, 'Partition to read from'],
      ['offset', 'int', true, 'Exact offset'],
      ['context', 'int', false, 'Also return this many messages either side'],
      ['max_value_bytes', 'int', false, 'Value bytes to return. Default 4096'],
    ],
    call: { items: [{ topic: 'orders', partition: 3, offset: 48211, context: 1 }] },
    result: {
      results: [{ index: 0, result: { topic: 'orders', message: { partition: 3, offset: 48211, key: 'ORD-12345' }, before: ['…'], after: ['…'] } }],
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
    name: 'commit_offset',
    group: 'change',
    batch: 100,
    write: 'own',
    summary: 'Moves a group\u2019s committed offset, forward to skip or back to replay.',
    detail:
      'The group must have no active members: a running consumer keeps its position in memory and overwrites the commit. To skip offset 42, commit 43.',
    params: [
      ['topic', 'string', true, 'Topic whose offset moves'],
      ['group', 'string', true, 'Consumer group'],
      ['partition', 'int', true, 'Partition'],
      ['offset', 'int', true, 'The offset the group reads next'],
      ['allow_active_members', 'bool', false, 'Proceed despite running consumers'],
    ],
    call: { items: [{ topic: 'orders', group: 'payments', partition: 0, offset: 43 }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        topic: 'orders', group: 'payments', state: 'Empty', current_offset: 42, requested_offset: 43,
        skipped_messages: 1, applied: false,
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
    name: 'copy_message',
    group: 'change',
    batch: 20,
    write: 'destination',
    summary: 'Copies a message by its address, to a dead letter topic or into another cluster.',
    detail:
      'Key, value and headers are preserved, and provenance headers record where it came from. A read-only endpoint can still be the source; only the destination must be writable.',
    params: [
      ['source_topic', 'string', true, 'Message to copy'],
      ['source_partition', 'int', true, ''],
      ['source_offset', 'int', true, ''],
      ['destination_topic', 'string', true, 'Must already exist'],
      ['destination_cluster', 'string', false, 'Defaults to this endpoint\u2019s cluster'],
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
      'Omit partition unless the exact partition is the point: the key decides placement, and a named partition breaks ordering for that key. A produced message cannot be deleted.',
    params: [
      ['topic', 'string', true, 'Existing topic'],
      ['value', 'string', true, 'The message body'],
      ['key', 'string', false, 'Decides the partition'],
      ['headers', 'object', false, 'Header name to value'],
      ['partition', 'int', false, 'Exact partition'],
      ['encoding', 'string', false, 'utf8 (default) or base64'],
      ['destination_cluster', 'string', false, 'Defaults to this endpoint\u2019s cluster'],
    ],
    call: { items: [{ topic: 'orders', key: 'ORD-12345', value: '{"status":"NEW"}' }], confirm: false },
    result: {
      results: [{ index: 0, result: {
        destination_cluster: 'preprod', topic: 'orders', applied: false,
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
    flow: ['consumer_lag', 'get_message', 'copy_message', 'commit_offset'],
    note: 'Keeps the bad message in a dead letter topic before moving the offset past it.',
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
    flow: ['get_message', 'list_clusters', 'copy_message', 'produce_message'],
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
]
