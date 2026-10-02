package opentransactions_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/opentransactions"
)

type OpenTransactionsSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestOpenTransactionsSuite(t *testing.T) {
	suite.Run(t, new(OpenTransactionsSuite))
}

func (s *OpenTransactionsSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *OpenTransactionsSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *OpenTransactionsSuite) check(topic string) (opentransactions.Output, error) {
	s.T().Helper()

	out, err := opentransactions.Run(s.T().Context(), s.env.Admin(), opentransactions.Input{
		Items: []opentransactions.Item{{Topic: topic}},
	})
	if err != nil {
		return opentransactions.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return opentransactions.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

func (s *OpenTransactionsSuite) TestOpenTransactionIsReportedWithItsProducer() {
	topic := s.env.CreateTopicWithPartitions(s.T(), "txn-open", 2)
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "committed", Partition: 0},
		testenv.Message{Value: "committed", Partition: 0},
	)

	transactionalID := s.env.UniqueName("payments-writer")
	s.env.OpenTransaction(s.T(), topic, transactionalID, 3)

	out, err := s.check(topic)
	s.Require().NoError(err, "checking a topic with an open transaction must succeed")

	s.Run("the blocked partition is reported", func() {
		s.Require().True(out.Blocked, "an open transaction holds read_committed consumers back, which is the point of the check")
		s.Require().Len(out.Partitions, 1, "only partition 0 holds the transaction; partition 1 must not be reported")

		partition := out.Partitions[0]
		s.Require().EqualValues(0, partition.Partition, "the partition must be named")
		s.Require().EqualValues(2, partition.LastStableOffset,
			"read_committed consumers stop at the first message of the open transaction, offset 2")
		// Redpanda reports one offset more than the records produced while a
		// transaction is open, so the end is asserted as a lower bound.
		s.Require().GreaterOrEqual(partition.HighWatermark, int64(5),
			"the partition holds two committed and three uncommitted messages")
		s.Require().Equal(partition.HighWatermark-partition.LastStableOffset, partition.UnreadableMessages,
			"everything between the stable offset and the end is invisible to read_committed consumers")
	})

	s.Run("the producer holding it is identified", func() {
		s.Require().Len(out.Partitions[0].Producers, 1, "one producer holds the transaction open")

		producer := out.Partitions[0].Producers[0]
		s.Require().EqualValues(2, producer.TransactionStartOffset,
			"the transaction started at offset 2, which is where consumers are stuck")
		s.Require().Equal(transactionalID, producer.TransactionalID,
			"the transactional id is how the operator finds which application owns the stuck producer")
		s.Require().Equal("Ongoing", producer.State, "the transaction has not been committed or aborted")
		s.Require().NotNil(producer.OpenFor, "how long it has been open tells a hung producer from a slow one")
		s.Require().NotNil(producer.TimeoutMillis, "the timeout says when the broker will abort it on its own")
	})
}

func (s *OpenTransactionsSuite) TestTopicWithoutTransactionsIsNotBlocked() {
	topic := s.env.CreateTopic(s.T(), "txn-none")
	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"}, testenv.Message{Value: "two"})

	out, err := s.check(topic)
	s.Require().NoError(err, "checking a topic without transactions must succeed")
	s.Require().False(out.Blocked, "a topic with no open transaction holds nobody back")
	s.Require().Empty(out.Partitions, "no partition is blocked")
	s.Require().NotNil(out.Partitions, "no partitions must be [] rather than null")
}

func (s *OpenTransactionsSuite) TestAbortedTransactionNoLongerBlocks() {
	topic := s.env.CreateTopic(s.T(), "txn-aborted")

	end := s.env.OpenTransaction(s.T(), topic, s.env.UniqueName("aborting-writer"), 2)
	end()

	// The abort marker is written asynchronously, so give the broker a moment
	// to advance the stable offset before asserting it has.
	s.Require().Eventually(func() bool {
		out, err := s.check(topic)

		return err == nil && !out.Blocked
	}, 10*time.Second, 100*time.Millisecond,
		"once the transaction ends the stable offset must catch up, and the topic must no longer be reported as blocked")
}

func (s *OpenTransactionsSuite) TestErrorsOnUnknownTopic() {
	_, err := s.check(s.env.UniqueName("missing"))

	s.Require().Error(err, "a topic that does not exist must fail rather than report no open transactions")
}

func (s *OpenTransactionsSuite) TestBatchKeepsOrderAndIsolatesErrors() {
	topic := s.env.CreateTopic(s.T(), "txn-batch")

	out, err := opentransactions.Run(s.T().Context(), s.env.Admin(), opentransactions.Input{
		Items: []opentransactions.Item{{Topic: s.env.UniqueName("missing")}, {Topic: topic}},
	})

	s.Require().NoError(err, "a structurally valid batch must return per-item results")
	s.Require().NotEmpty(out.Results[0].Error, "the missing topic must fail on its own item")
	s.Require().NotNil(out.Results[1].Result, "the real topic must still be checked")
	s.Require().Equal(topic, out.Results[1].Result.Topic, "results must stay in input order")
}

func (s *OpenTransactionsSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kafkaclient.New(&config.Cluster{Name: "test", Brokers: []string{"127.0.0.1:1"}})
	s.Require().NoError(err, "building a client against a dead address must not fail yet")
	s.T().Cleanup(client.Close)

	out, err := opentransactions.Run(s.T().Context(), client.Admin(), opentransactions.Input{
		Items: []opentransactions.Item{{Topic: "anything"}},
	})

	s.Require().NoError(err, "a failure to reach the broker belongs to the item, so the call still returns results")
	s.Require().NotEmpty(out.Results[0].Error,
		"an unreachable broker must be an error, never a report that nothing is blocked")
}
