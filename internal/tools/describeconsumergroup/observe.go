package describeconsumergroup

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// maxSampleSeconds bounds how long one observation holds the request open.
const maxSampleSeconds = 60

// observeInterval is how often the group is read during an observation. A
// rebalance usually completes within a few seconds, so a coarser interval
// would miss short ones entirely.
const observeInterval = time.Second

// observe reads the group repeatedly for seconds and reports what changed.
//
// Classic DescribeGroups carries no generation id, so churn is detected from
// the state and from member ids: a restarting consumer rejoins with a new id,
// and shows up as one member leaving and another joining.
func observe(ctx context.Context, admin *kadm.Client, group string, seconds int) (*Observation, error) {
	observation := &Observation{Seconds: seconds, States: []string{}, Joined: []Member{}, Left: []Member{}}

	first := map[string]Member{}
	ever := map[string]Member{}
	var previous map[string]Member

	deadline := time.Now().Add(time.Duration(seconds) * time.Second)

	for {
		members, state, err := snapshot(ctx, admin, group)
		if err != nil {
			return nil, err
		}

		observation.Samples++

		if len(observation.States) == 0 || observation.States[len(observation.States)-1] != state {
			observation.States = append(observation.States, state)
		}

		if previous == nil {
			first = members
		}

		for id, member := range members {
			ever[id] = member
		}

		previous = members

		if !time.Now().Before(deadline) {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(observeInterval):
		}
	}

	for id, member := range ever {
		_, atStart := first[id]
		_, atEnd := previous[id]

		if !atStart {
			observation.Joined = append(observation.Joined, member)
		}

		if !atEnd {
			observation.Left = append(observation.Left, member)
		}
	}

	byID := func(members []Member) {
		sort.Slice(members, func(i, j int) bool { return members[i].MemberID < members[j].MemberID })
	}
	byID(observation.Joined)
	byID(observation.Left)

	observation.Unstable = len(observation.Joined) > 0 || len(observation.Left) > 0 ||
		len(observation.States) > 1 || (len(observation.States) == 1 && observation.States[0] != "Stable" &&
		observation.States[0] != "Empty")

	return observation, nil
}

// snapshot returns the group's members by id, and its state.
func snapshot(ctx context.Context, admin *kadm.Client, group string) (map[string]Member, string, error) {
	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return nil, "", fmt.Errorf("describe group %q: %w", group, err)
	}

	detail, ok := described[group]
	if !ok {
		return nil, "", fmt.Errorf("consumer group %q does not exist", group)
	}

	if detail.Err != nil {
		return nil, "", fmt.Errorf("consumer group %q: %w", group, detail.Err)
	}

	if detail.State == "Dead" {
		return nil, "", fmt.Errorf("consumer group %q does not exist: the broker reports it as Dead", group)
	}

	members := make(map[string]Member, len(detail.Members))

	for _, member := range detail.Members {
		entry := Member{
			MemberID:    member.MemberID,
			ClientID:    member.ClientID,
			Host:        member.ClientHost,
			Assignments: []Assignment{},
		}

		if member.InstanceID != nil {
			entry.InstanceID = *member.InstanceID
		}

		members[member.MemberID] = entry
	}

	return members, detail.State, nil
}
