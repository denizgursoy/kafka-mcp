// Package testenv starts the container stack used by the test suites in this
// repository: one Redpanda broker and one Redpanda Console, wired together on
// a dedicated Docker network.
//
// The package is test-only infrastructure. Its entrypoint takes nothing but a
// *testing.T, so suites do not have to know about Docker, networks, images or
// wait strategies:
//
//	func (s *MySuite) SetupSuite() {
//	    s.env = testenv.Start(s.T())
//	}
//
//	func (s *MySuite) TearDownSuite() {
//	    s.env.Stop()
//	}
//
// If the containers cannot be started, the reason is logged and the test is
// failed, so a broken Docker setup can never be mistaken for passing tests.
// When the containers do start, the connection details are logged so a human
// can attach to the same broker or open the Console while the suite runs.
package testenv

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/denizgursoy/kafka-mcp/internal/domain/records"
)

const (
	// BrokerImage is the Redpanda broker image started for tests.
	BrokerImage = "redpandadata/redpanda:v25.3.1"

	// ConsoleImage is the Redpanda Console image started for tests.
	ConsoleImage = "redpandadata/console:v3.3.0"

	// networkAlias is the hostname the broker gets on the test network, so the
	// Console can reach it without going through the host.
	networkAlias = "redpanda"

	// internalListener is the broker listener the Console connects to. It is
	// only reachable from inside the Docker network.
	internalListener = "redpanda:29092"

	startTimeout = 3 * time.Minute

	// metadataMinAge bounds how stale franz-go's cached metadata may be. See
	// the comment where the client is built for why the default is unusable in
	// tests.
	metadataMinAge = 50 * time.Millisecond
)

// Environment is a running Redpanda broker plus Console. Create it with Start
// and release it with Stop.
type Environment struct {
	t *testing.T

	network *testcontainers.DockerNetwork
	broker  *redpanda.Container
	console testcontainers.Container
	client  *kgo.Client
	admin   *kadm.Client

	skipConsole bool

	mu     sync.Mutex
	topics []string
	groups []string

	seed           string
	consoleURL     string
	schemaRegistry string
	adminAPI       string
}

// Start brings up a Redpanda broker and a Redpanda Console and connects a
// Kafka client to the broker.
//
// Start fails the test if anything in that chain fails, after logging the
// reason. Any containers that did start are torn down first, so a failure
// never leaks containers. On success the connection details are logged.
func Start(t *testing.T) *Environment {
	t.Helper()

	return start(t)
}

func start(t *testing.T, options ...startOption) *Environment {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping container test in short mode")
	}

	// The start context is derived from the test's own context, so abandoning
	// the test also abandons container startup.
	ctx, cancel := context.WithTimeout(t.Context(), startTimeout)
	defer cancel()

	env := &Environment{t: t}

	for _, option := range options {
		option(env)
	}

	if err := env.start(ctx); err != nil {
		t.Logf("test environment did not start: %v", err)

		env.Stop()

		t.Fatalf("start test environment: %v", err)
	}

	env.logConnectionDetails()

	return env
}

// StartPair brings up two independent Redpanda brokers, so a test can prove
// that something genuinely crosses a cluster boundary rather than merely
// moving between two topics of one broker.
//
// Only the first environment runs a Console: it exists for a human watching a
// suite, and a second one would double the startup cost for no benefit.
func StartPair(t *testing.T) (*Environment, *Environment) {
	t.Helper()

	first := Start(t)

	second := start(t, withoutConsole)

	return first, second
}

// startOption varies what an environment brings up.
type startOption func(*Environment)

// withoutConsole skips the Console container, which is only useful to a human
// and costs startup time a second broker does not need.
func withoutConsole(e *Environment) {
	e.skipConsole = true
}

func (e *Environment) start(ctx context.Context) error {
	var err error

	e.network, err = network.New(ctx)
	if err != nil {
		return fmt.Errorf("create docker network: %w", err)
	}

	e.broker, err = redpanda.Run(
		ctx,
		BrokerImage,
		redpanda.WithListener(internalListener),
		network.WithNetwork([]string{networkAlias}, e.network),
	)
	if err != nil {
		return fmt.Errorf("start broker %s: %w", BrokerImage, err)
	}

	e.seed, err = e.broker.KafkaSeedBroker(ctx)
	if err != nil {
		return fmt.Errorf("resolve kafka seed broker: %w", err)
	}

	e.schemaRegistry, err = e.broker.SchemaRegistryAddress(ctx)
	if err != nil {
		return fmt.Errorf("resolve schema registry address: %w", err)
	}

	e.adminAPI, err = e.broker.AdminAPIAddress(ctx)
	if err != nil {
		return fmt.Errorf("resolve admin api address: %w", err)
	}

	if !e.skipConsole {
		if err := e.startConsole(ctx); err != nil {
			return err
		}
	}

	// kadm.ListTopics answers unfiltered listings from franz-go's metadata
	// cache, which is MetadataMinAge (5s) old by default. A topic created by a
	// test would then be invisible to the code under test for several seconds.
	// Tests need to observe writes immediately, so shrink the cache window.
	//
	// ManualPartitioner makes Record.Partition authoritative. Without it
	// franz-go ignores that field and balances records itself, so a test that
	// produces to a named partition would silently land somewhere else.
	e.client, err = kgo.NewClient(
		kgo.SeedBrokers(e.seed),
		kgo.MetadataMinAge(metadataMinAge),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		return fmt.Errorf("connect kafka client to %s: %w", e.seed, err)
	}

	e.admin = kadm.NewClient(e.client)

	if _, err := e.admin.ListTopics(ctx); err != nil {
		return fmt.Errorf("broker %s did not answer metadata request: %w", e.seed, err)
	}

	return nil
}

func (e *Environment) startConsole(ctx context.Context) error {
	console, err := testcontainers.Run(
		ctx,
		ConsoleImage,
		testcontainers.WithEnv(map[string]string{
			"KAFKA_BROKERS":                internalListener,
			"KAFKA_SCHEMAREGISTRY_ENABLED": "true",
			"KAFKA_SCHEMAREGISTRY_URLS":    "http://" + networkAlias + ":8081",
		}),
		testcontainers.WithExposedPorts("8080/tcp"),
		network.WithNetwork([]string{"console"}, e.network),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/admin/health").
				WithPort("8080/tcp").
				WithStartupTimeout(time.Minute),
		),
	)
	if err != nil {
		return fmt.Errorf("start console %s: %w", ConsoleImage, err)
	}

	e.console = console

	endpoint, err := console.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		return fmt.Errorf("resolve console endpoint: %w", err)
	}

	e.consoleURL = endpoint

	return nil
}

func (e *Environment) logConnectionDetails() {
	e.t.Helper()

	if e.skipConsole {
		e.t.Logf("test environment ready: kafka broker %s, schema registry %s, admin api %s",
			e.seed, e.schemaRegistry, e.adminAPI)

		return
	}

	e.t.Logf(
		"test environment ready: kafka broker %s, schema registry %s, admin api %s, console %s",
		e.seed,
		e.schemaRegistry,
		e.adminAPI,
		e.consoleURL,
	)
}

// Stop releases everything Start created: the Kafka client, the Console, the
// broker and the network. It is safe to call on a partially started
// environment and safe to call more than once.
//
// Failures during teardown are logged rather than failing the test, since a
// test that already passed should not be turned red by a slow container.
func (e *Environment) Stop() {
	e.t.Helper()

	// Teardown deliberately does not use t.Context: that context is canceled
	// as the test finishes, which would abort the cleanup it is meant to do.
	ctx := context.Background()

	e.deleteTrackedGroups(ctx)
	e.deleteTrackedTopics(ctx)

	if e.client != nil {
		e.client.Close()
		e.client = nil
		e.admin = nil
	}

	if e.console != nil {
		if err := e.console.Terminate(ctx); err != nil {
			e.t.Logf("terminate console: %v", err)
		}

		e.console = nil
	}

	if e.broker != nil {
		if err := e.broker.Terminate(ctx); err != nil {
			e.t.Logf("terminate broker: %v", err)
		}

		e.broker = nil
	}

	if e.network != nil {
		if err := e.network.Remove(ctx); err != nil {
			e.t.Logf("remove network: %v", err)
		}

		e.network = nil
	}
}

// Admin returns the Kafka admin client connected to the running broker.
func (e *Environment) Admin() *kadm.Client {
	return e.admin
}

// Kafka returns the record-level client connected to the running broker.
func (e *Environment) Kafka() *kgo.Client {
	return e.client
}

// Broker returns the host address of the Kafka listener, suitable for
// kgo.SeedBrokers.
func (e *Environment) Broker() string {
	return e.seed
}

// Reader returns a record reader pointed at the running broker, for tools that
// read message content rather than metadata.
func (e *Environment) Reader() *records.Reader {
	return records.NewReader(e.seed)
}

// SchemaRegistry returns the host address of the Schema Registry.
func (e *Environment) SchemaRegistry() string {
	return e.schemaRegistry
}

// AdminAPI returns the host address of the Redpanda Admin API.
func (e *Environment) AdminAPI() string {
	return e.adminAPI
}

// ConsoleURL returns the browser URL of the Redpanda Console attached to the
// broker.
func (e *Environment) ConsoleURL() string {
	return e.consoleURL
}

// CreateTopic creates a single-partition topic whose name starts with prefix
// and ends with a unique suffix, and returns the generated name.
//
// The unique suffix matters because suites share one broker across their test
// cases: reusing a fixed name would let one case see another's topics. The
// topic is deleted by Stop.
//
// Pass the running test's own *testing.T, not the suite's, so a creation
// failure aborts the test that is actually running.
func (e *Environment) CreateTopic(t *testing.T, prefix string) string {
	t.Helper()

	return e.CreateTopics(t, prefix)[0]
}

// CreateTopics creates one single-partition topic per prefix and returns the
// generated names in the same order. The topics are deleted by Stop.
//
// Cleanup is deferred to Stop rather than registered with t.Cleanup because a
// suite's Stop runs in TearDownSuite, before the cleanups of the suite-level
// *testing.T. Deleting there would use a client Stop has already closed.
func (e *Environment) CreateTopics(t *testing.T, prefixes ...string) []string {
	t.Helper()

	names := make([]string, 0, len(prefixes))

	for _, prefix := range prefixes {
		names = append(names, e.UniqueName(prefix))
	}

	responses, err := e.admin.CreateTopics(t.Context(), 1, 1, nil, names...)
	if err != nil {
		t.Fatalf("create topics %v: %v", names, err)
	}

	for _, response := range responses {
		if response.Err != nil {
			t.Fatalf("create topic %s: %v", response.Topic, response.Err)
		}
	}

	e.mu.Lock()
	e.topics = append(e.topics, names...)
	e.mu.Unlock()

	return names
}

// DeleteTopics removes the given topics. Deletion failures are logged, not
// fatal, so cleanup never masks the real assertion failure of a test.
func (e *Environment) DeleteTopics(t *testing.T, topics ...string) {
	t.Helper()

	e.deleteTopics(t.Context(), topics...)
}

func (e *Environment) deleteTopics(ctx context.Context, topics ...string) {
	e.t.Helper()

	if len(topics) == 0 || e.admin == nil {
		return
	}

	if _, err := e.admin.DeleteTopics(ctx, topics...); err != nil {
		e.t.Logf("delete topics %v: %v", topics, err)
	}
}

// deleteTrackedGroups removes the consumer groups the tests created. Groups
// are deleted before topics, because deleting a topic a group still has
// commits for leaves those commits behind on the cluster.
func (e *Environment) deleteTrackedGroups(ctx context.Context) {
	e.t.Helper()

	e.mu.Lock()
	groups := e.groups
	e.groups = nil
	e.mu.Unlock()

	if len(groups) == 0 || e.admin == nil {
		return
	}

	if _, err := e.admin.DeleteGroups(ctx, groups...); err != nil {
		e.t.Logf("delete groups %v: %v", groups, err)
	}
}

func (e *Environment) deleteTrackedTopics(ctx context.Context) {
	e.t.Helper()

	e.mu.Lock()
	topics := e.topics
	e.topics = nil
	e.mu.Unlock()

	e.deleteTopics(ctx, topics...)
}

// UniqueName appends a unique suffix to prefix, for naming topics or groups
// that must not collide with other test cases sharing the broker.
func (e *Environment) UniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// Message describes a record to produce in a test. Every field is optional
// except Value, so a test only states what it actually cares about.
type Message struct {
	Key       string
	Value     string
	Headers   map[string]string
	Partition int32
	Timestamp time.Time
}

// Produce writes the given messages to a topic and waits for the broker to
// acknowledge them, so a test that produces then searches cannot race.
//
// It returns the offset assigned to each message, in the order given, because
// tests assert on the exact offsets a tool reports back.
func (e *Environment) Produce(t *testing.T, topic string, messages ...Message) []int64 {
	t.Helper()

	records := make([]*kgo.Record, 0, len(messages))

	for _, message := range messages {
		record := &kgo.Record{
			Topic:     topic,
			Partition: message.Partition,
			Value:     []byte(message.Value),
		}

		if message.Key != "" {
			record.Key = []byte(message.Key)
		}

		if !message.Timestamp.IsZero() {
			record.Timestamp = message.Timestamp
		}

		for key, value := range message.Headers {
			record.Headers = append(record.Headers, kgo.RecordHeader{
				Key:   key,
				Value: []byte(value),
			})
		}

		// Headers come from a map, so sort them to keep produced records
		// byte-identical across runs.
		sort.Slice(record.Headers, func(i, j int) bool {
			return record.Headers[i].Key < record.Headers[j].Key
		})

		records = append(records, record)
	}

	results := e.client.ProduceSync(t.Context(), records...)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("produce %d messages to %s: %v", len(messages), topic, err)
	}

	offsets := make([]int64, 0, len(records))

	for _, record := range records {
		offsets = append(offsets, record.Offset)
	}

	return offsets
}

// CreateTopicWithPartitions creates a topic with the given partition count and
// returns its generated name, for tests that need to assert across partitions.
func (e *Environment) CreateTopicWithPartitions(t *testing.T, prefix string, partitions int32) string {
	t.Helper()

	name := e.UniqueName(prefix)

	responses, err := e.admin.CreateTopics(t.Context(), partitions, 1, nil, name)
	if err != nil {
		t.Fatalf("create topic %s with %d partitions: %v", name, partitions, err)
	}

	for _, response := range responses {
		if response.Err != nil {
			t.Fatalf("create topic %s: %v", response.Topic, response.Err)
		}
	}

	e.mu.Lock()
	e.topics = append(e.topics, name)
	e.mu.Unlock()

	return name
}

// ConsumeAndCommit consumes exactly count records from a topic as a member of
// the given consumer group, commits them, and returns the group name.
//
// It uses a real consumer rather than writing offsets directly, so the group
// exists the way a production group does. Autocommit is disabled and the
// commit is synchronous, so when this returns the committed offset is exactly
// count and the lag is exactly whatever remains: no waiting on a timer, and
// nothing timing-dependent for a test to race against.
//
// The client is closed before returning, which leaves the group in the Empty
// state with its commits intact. That is the same state a group reaches when
// its consumers stop, so a test can assert on lag without an active member.
func (e *Environment) ConsumeAndCommit(
	t *testing.T,
	topic string,
	group string,
	count int,
) {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(e.seed),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatalf("connect consumer for group %s: %v", group, err)
	}

	defer client.Close()

	consumed := make([]*kgo.Record, 0, count)

	for len(consumed) < count {
		fetches := client.PollRecords(t.Context(), count-len(consumed))

		if err := fetches.Err0(); err != nil {
			t.Fatalf("consume from %s as group %s: %v", topic, group, err)
		}

		fetches.EachRecord(func(record *kgo.Record) {
			if len(consumed) < count {
				consumed = append(consumed, record)
			}
		})
	}

	if err := client.CommitRecords(t.Context(), consumed...); err != nil {
		t.Fatalf("commit %d records for group %s: %v", len(consumed), group, err)
	}

	e.mu.Lock()
	e.groups = append(e.groups, group)
	e.mu.Unlock()
}

// CreateTopicWithConfig creates a single-partition topic carrying the given
// topic-level configuration, and returns its generated name.
//
// Tests need this to tell a config set on the topic apart from one inherited
// from the cluster: the two look identical in a config listing except for the
// source Kafka reports.
func (e *Environment) CreateTopicWithConfig(
	t *testing.T,
	prefix string,
	configs map[string]string,
) string {
	t.Helper()

	name := e.UniqueName(prefix)

	// kadm takes pointers so that a nil value can mean "delete this config",
	// which is why the values cannot be passed as a plain string map.
	values := make(map[string]*string, len(configs))

	for key, value := range configs {
		values[key] = kadm.StringPtr(value)
	}

	responses, err := e.admin.CreateTopics(t.Context(), 1, 1, values, name)
	if err != nil {
		t.Fatalf("create topic %s with config %v: %v", name, configs, err)
	}

	for _, response := range responses {
		if response.Err != nil {
			t.Fatalf("create topic %s: %v", response.Topic, response.Err)
		}
	}

	e.mu.Lock()
	e.topics = append(e.topics, name)
	e.mu.Unlock()

	return name
}
