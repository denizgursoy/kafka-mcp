package listacls_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listacls"
)

type ListACLsSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestListACLsSuite(t *testing.T) {
	suite.Run(t, new(ListACLsSuite))
}

func (s *ListACLsSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *ListACLsSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *ListACLsSuite) list(input listacls.Input) listacls.Output {
	s.T().Helper()

	out, err := listacls.Run(s.T().Context(), s.env.Admin(), input)
	s.Require().NoError(err, "listing ACLs on a broker that stores them must succeed")

	return out
}

func (s *ListACLsSuite) TestListsTheACLsOfAPrincipal() {
	principal := "User:" + s.env.UniqueName("alice")
	topic := s.env.UniqueName("orders")

	s.env.CreateACL(s.T(), testenv.ACL{Principal: principal, Topic: topic, Operation: kadm.OpRead})
	s.env.CreateACL(s.T(), testenv.ACL{Principal: principal, Topic: topic, Operation: kadm.OpWrite, Deny: true})

	out := s.list(listacls.Input{Principal: principal})

	s.Require().Len(out.ACLs, 2, "both entries for the principal must be listed, and nothing for anyone else")
	s.Require().Equal(2, out.Count, "count must match the list it summarises")

	read := out.ACLs[0]
	s.Require().Equal(principal, read.Principal, "every entry must name its principal")
	s.Require().Equal("topic", read.ResourceType, "the resource type is reported in lower case words, not as a protocol number")
	s.Require().Equal(topic, read.ResourceName, "the resource name must be reported")
	s.Require().Equal("literal", read.PatternType, "a literal ACL matches only that exact name")
	s.Require().Equal("read", read.Operation, "the operation is what the caller compares against the failing request")
	s.Require().Equal("allow", read.Permission, "allow and deny must be told apart, since a deny overrides every allow")

	s.Require().Equal("write", out.ACLs[1].Operation, "entries are sorted, so read comes before write")
	s.Require().Equal("deny", out.ACLs[1].Permission, "the deny must be reported as one")
}

func (s *ListACLsSuite) TestResourceFilterIncludesPrefixedACLsThatMatch() {
	principal := "User:" + s.env.UniqueName("bob")
	prefix := s.env.UniqueName("payments")
	topic := prefix + "-events"

	s.env.CreateACL(s.T(), testenv.ACL{Principal: principal, Topic: prefix, Prefixed: true, Operation: kadm.OpRead})

	out := s.list(listacls.Input{ResourceType: "topic", ResourceName: topic})

	found := slices.ContainsFunc(out.ACLs, func(acl listacls.ACL) bool {
		return acl.Principal == principal && acl.PatternType == "prefixed" && acl.ResourceName == prefix
	})

	s.Require().True(found,
		"asking about a topic must include prefixed ACLs that cover it, because those are what the broker applies to it")
}

func (s *ListACLsSuite) TestResourceTypeIsCaseInsensitive() {
	principal := "User:" + s.env.UniqueName("carol")
	topic := s.env.UniqueName("audit")

	s.env.CreateACL(s.T(), testenv.ACL{Principal: principal, Topic: topic, Operation: kadm.OpDescribe})

	out := s.list(listacls.Input{ResourceType: "TOPIC", ResourceName: topic})

	s.Require().Len(out.ACLs, 1, "resource_type is documented as case-insensitive")
}

func (s *ListACLsSuite) TestNoMatchReturnsAnEmptyList() {
	out := s.list(listacls.Input{Principal: "User:" + s.env.UniqueName("nobody")})

	s.Require().Empty(out.ACLs, "a principal with no ACLs has none to list")
	s.Require().NotNil(out.ACLs, "no ACLs must be [] rather than null so callers can iterate safely")
}

func (s *ListACLsSuite) TestRefusesAnUnknownResourceType() {
	_, err := listacls.Run(s.T().Context(), s.env.Admin(), listacls.Input{ResourceType: "table"})

	s.Require().Error(err, "an unknown resource type must be refused rather than silently match everything")
}

func (s *ListACLsSuite) TestRefusesAResourceNameWithoutAType() {
	_, err := listacls.Run(s.T().Context(), s.env.Admin(), listacls.Input{ResourceName: "orders"})

	s.Require().Error(err,
		"a topic and a group may share a name, so a resource name without a type is ambiguous")
}

func (s *ListACLsSuite) TestErrorsWhenBrokerUnreachable() {
	client, err := kafkaclient.New(&config.Cluster{Name: "test", Brokers: []string{"127.0.0.1:1"}})
	s.Require().NoError(err, "building a client against a dead address must not fail yet")
	s.T().Cleanup(client.Close)

	_, err = listacls.Run(s.T().Context(), client.Admin(), listacls.Input{})
	s.Require().Error(err, "an unreachable broker must be an error, never an empty ACL list that reads as 'nothing is restricted'")
}
