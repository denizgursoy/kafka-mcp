package deleteconsumergroup_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/denizgursoy/kafka-mcp/internal/domain/testenv"
	"github.com/denizgursoy/kafka-mcp/internal/tools/deleteconsumergroup"
)

type DeleteConsumerGroupSuite struct {
	suite.Suite

	env *testenv.Environment
}

func TestDeleteConsumerGroupSuite(t *testing.T) {
	suite.Run(t, new(DeleteConsumerGroupSuite))
}

func (s *DeleteConsumerGroupSuite) SetupSuite() {
	s.env = testenv.Start(s.T())
}

func (s *DeleteConsumerGroupSuite) TearDownSuite() {
	s.env.Stop()
}

func (s *DeleteConsumerGroupSuite) deleteOne(
	readOnly bool,
	confirm bool,
	group string,
) (deleteconsumergroup.Output, error) {
	s.T().Helper()

	out, err := deleteconsumergroup.Run(
		s.T().Context(),
		s.env.ClusterClient(s.T(), readOnly),
		deleteconsumergroup.Input{Items: []deleteconsumergroup.Item{{Group: group}}, Confirm: confirm},
	)
	if err != nil {
		return deleteconsumergroup.Output{}, err
	}

	s.Require().Len(out.Results, 1,
		"one item in must produce exactly one result out, or results cannot be matched to inputs by position")

	if out.Results[0].Error != "" {
		return deleteconsumergroup.Output{}, errors.New(out.Results[0].Error)
	}

	return *out.Results[0].Result, nil
}

// groupExists asks the broker directly, so a test proves what happened rather
// than trusting the tool's report.
func (s *DeleteConsumerGroupSuite) groupExists(group string) bool {
	s.T().Helper()

	described, err := s.env.Admin().DescribeGroups(s.T().Context(), group)
	s.Require().NoError(err, "describing the group must succeed")

	detail, ok := described[group]

	return ok && detail.State != "Dead"
}

// abandonedGroup leaves a group with commits and no members, which is what a
// consumer that was decommissioned looks like.
func (s *DeleteConsumerGroupSuite) abandonedGroup(prefix string) (string, string) {
	s.T().Helper()

	topic := s.env.CreateTopic(s.T(), prefix)
	s.env.Produce(s.T(), topic,
		testenv.Message{Value: "one"}, testenv.Message{Value: "two"}, testenv.Message{Value: "three"})

	group := s.env.UniqueName(prefix + "-group")
	s.env.ConsumeAndCommit(s.T(), topic, group, 1)

	return topic, group
}

func (s *DeleteConsumerGroupSuite) TestPreviewDeletesNothing() {
	topic, group := s.abandonedGroup("delete-group-preview")

	out, err := s.deleteOne(false, false, group)
	s.Require().NoError(err, "previewing the deletion of an empty group must succeed")

	s.Run("the group still exists", func() {
		s.Require().True(s.groupExists(group),
			"a preview must change nothing: deleting a group loses its position, which cannot be recovered")
	})

	s.Run("the preview shows what would be lost", func() {
		s.Require().False(out.Deleted, "a preview must be told apart from a deletion that happened")
		s.Require().Equal("Empty", out.State, "the state is reported so the caller sees the group is not running")
		s.Require().Len(out.Offsets, 1, "every committed position that would be lost must be listed")
		s.Require().Equal(topic, out.Offsets[0].Topic, "the offset must name its topic")
		s.Require().EqualValues(1, out.Offsets[0].CommittedOffset, "the committed offset is the position being thrown away")
		s.Require().EqualValues(2, out.TotalLag,
			"the lag being cleared must be shown, because an alert that disappears with the group should be a deliberate choice")
	})
}

func (s *DeleteConsumerGroupSuite) TestConfirmDeletesAnEmptyGroup() {
	_, group := s.abandonedGroup("delete-group-confirm")

	out, err := s.deleteOne(false, true, group)
	s.Require().NoError(err, "deleting an empty group must succeed")
	s.Require().True(out.Deleted, "a deletion that happened must be reported as such")
	s.Require().False(s.groupExists(group), "the broker must no longer know the group, which is the only proof it was deleted")
}

func (s *DeleteConsumerGroupSuite) TestRefusesAGroupWithActiveMembers() {
	topic := s.env.CreateTopic(s.T(), "delete-group-active")
	s.env.Produce(s.T(), topic, testenv.Message{Value: "one"})

	group := s.env.UniqueName("delete-group-active")
	s.env.JoinGroup(s.T(), topic, group, "running-worker")

	_, err := s.deleteOne(false, true, group)
	s.Require().Error(err,
		"a running group is not abandoned, and deleting it would reset where its consumers resume after their next restart")
	s.Require().True(s.groupExists(group), "a refused deletion must leave the group in place")
}

func (s *DeleteConsumerGroupSuite) TestErrorsOnUnknownGroup() {
	_, err := s.deleteOne(false, false, s.env.UniqueName("never-existed"))

	s.Require().Error(err, "a group that does not exist must fail, since a typo should not read as a successful clean-up")
}

func (s *DeleteConsumerGroupSuite) TestReadOnlyRefusesEvenThePreview() {
	_, group := s.abandonedGroup("delete-group-read-only")

	_, err := s.deleteOne(true, false, group)
	s.Require().Error(err,
		"deleting is this tool's only purpose, so a read-only server refuses the preview too")
	s.Require().Contains(err.Error(), "read-only", "the error must name the reason")
	s.Require().True(s.groupExists(group), "nothing may have changed")
}

func (s *DeleteConsumerGroupSuite) TestBatchDeletesValidGroupsAndReportsTheRest() {
	_, first := s.abandonedGroup("delete-group-batch-a")
	_, second := s.abandonedGroup("delete-group-batch-b")

	out, err := deleteconsumergroup.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), deleteconsumergroup.Input{
		Items: []deleteconsumergroup.Item{
			{Group: first},
			{Group: s.env.UniqueName("missing")},
			{Group: second},
		},
		Confirm: true,
	})

	s.Require().NoError(err, "a structurally valid batch must return per-item results")
	s.Require().Equal(2, out.Succeeded, "both real groups must be deleted")
	s.Require().Equal(1, out.Failed, "the missing group must be one failed item, not a failed call")
	s.Require().NotEmpty(out.Results[1].Error, "the failure must sit on its own item, in input order")
	s.Require().False(s.groupExists(first), "the first group must be gone")
	s.Require().False(s.groupExists(second), "the group after a failed item must still be deleted")
}

func (s *DeleteConsumerGroupSuite) TestRefusesDuplicateGroups() {
	_, group := s.abandonedGroup("delete-group-duplicate")

	_, err := deleteconsumergroup.Run(s.T().Context(), s.env.ClusterClient(s.T(), false), deleteconsumergroup.Input{
		Items:   []deleteconsumergroup.Item{{Group: group}, {Group: group}},
		Confirm: true,
	})

	s.Require().Error(err, "naming one group twice means the caller has lost track of what they are deleting")
	s.Require().True(s.groupExists(group), "a refused batch must delete nothing")
}
