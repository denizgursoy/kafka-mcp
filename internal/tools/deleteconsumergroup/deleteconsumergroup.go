// Package deleteconsumergroup implements the delete_consumer_group MCP tool.
package deleteconsumergroup

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/denizgursoy/kafka-mcp/internal/domain/batch"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

const toolName = "delete_consumer_group"

// Item is one group to delete.
type Item struct {
	Group string `json:"group" jsonschema:"Consumer group to delete. Matched exactly and case-sensitively. It must exist and have no active members."`
}

// Input is the argument set accepted by the delete_consumer_group tool.
type Input struct {
	Items   []Item `json:"items" jsonschema:"The groups to delete, 1 to 100 of them. Deleting one group is an array of length one. Naming the same group twice is refused before anything is deleted."`
	Confirm bool   `json:"confirm,omitempty" jsonschema:"Optional. When false or omitted, nothing is deleted and the response shows each group's committed offsets and lag. Must be true to delete. One confirm covers the whole batch."`
}

// Offset is one committed position the deletion throws away.
type Offset struct {
	Topic           string `json:"topic"`
	Partition       int32  `json:"partition"`
	CommittedOffset int64  `json:"committed_offset"`
	EndOffset       int64  `json:"end_offset"`
	Lag             int64  `json:"lag"`
}

// Output is the result of one item.
type Output struct {
	Group    string   `json:"group"`
	State    string   `json:"state"`
	Members  int      `json:"members"`
	Topics   []string `json:"topics"`
	Offsets  []Offset `json:"offsets"`
	TotalLag int64    `json:"total_lag"`

	WouldDelete bool     `json:"would_delete"`
	Deleted     bool     `json:"deleted"`
	Warnings    []string `json:"warnings"`
	Note        string   `json:"note,omitempty"`
}

type BatchOutput = batch.Output[Output]

const description = `
Delete 1 to 100 consumer groups in one call through items, removing each
group's committed offsets. Use it to clean up groups whose consumers were
decommissioned: their commits keep reporting lag that nobody will ever drain.
Deleting one group is an items array of length one.

No group is deleted unless confirm is true. The preview lists each group's
state, the topics it has committed on, every committed offset with its lag,
and the total lag that would disappear with it. A group with active members is
refused: it is not abandoned, and deleting it would reset where its consumers
resume.

If a consumer later starts with the same group id, it begins wherever its
auto.offset.reset points, not where the group left off. One confirm covers the
whole batch, and deletion is not atomic: groups deleted before a later item
failed stay deleted. Results follow items order, each carrying index with
result or error. Requires Kafka DELETE permission on the group.
`

// Register adds the delete_consumer_group tool to the MCP server.
func Register(server *mcp.Server, kafka *kafkaclient.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        toolName,
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, BatchOutput, error) {

			out, err := Run(ctx, kafka, input)
			if err != nil {
				return nil, BatchOutput{}, fmt.Errorf("delete consumer group: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run previews every group before deleting any valid one.
func Run(ctx context.Context, kafka *kafkaclient.Client, input Input) (BatchOutput, error) {
	items, confirm := input.Items, input.Confirm

	// Deleting is this tool's only purpose, so a read-only endpoint is refused
	// outright rather than offered a preview of a change it can never apply.
	if err := kafka.RequireWritable(toolName); err != nil {
		return BatchOutput{}, err
	}

	if err := batch.Validate(len(items), batch.MaxItems); err != nil {
		return BatchOutput{}, err
	}

	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if _, ok := seen[item.Group]; ok {
			return BatchOutput{}, fmt.Errorf(
				"duplicate group %q in items: naming one group twice in a deletion means the caller has lost track of what they are removing",
				item.Group)
		}
		seen[item.Group] = struct{}{}
	}

	out, err := batch.Run(ctx, items, batch.MaxItems, func(ctx context.Context, item Item) (Output, error) {
		return preview(ctx, kafka.Admin(), item.Group)
	})
	if err != nil || !confirm {
		return out, err
	}

	out.Succeeded, out.Failed = 0, 0

	for index, item := range items {
		if out.Results[index].Error != "" {
			out.Failed++

			continue
		}

		// Re-checked immediately before deleting, because a consumer may have
		// joined since the preview was read.
		current, err := preview(ctx, kafka.Admin(), item.Group)
		if err == nil {
			err = remove(ctx, kafka.Admin(), item.Group)
		}

		if err != nil {
			out.Results[index].Result = nil
			out.Results[index].Error = err.Error()
			out.Failed++

			continue
		}

		current.Deleted = true
		current.WouldDelete = false
		current.Note = fmt.Sprintf(
			"deleted group %q and its %d committed offset(s). A consumer that later starts with this group id resumes from its auto.offset.reset",
			item.Group, len(current.Offsets))

		out.Results[index].Result = &current
		out.Succeeded++
		out.Applied++
	}

	return out, nil
}

func preview(ctx context.Context, admin *kadm.Client, group string) (Output, error) {
	if group == "" {
		return Output{}, fmt.Errorf("group is required")
	}

	described, err := admin.DescribeGroups(ctx, group)
	if err != nil {
		return Output{}, fmt.Errorf("describe group %q: %w", group, err)
	}

	detail, ok := described[group]
	if !ok || detail.State == "Dead" {
		return Output{}, fmt.Errorf(
			"consumer group %q does not exist: nothing was deleted, and a name that is not on this cluster is more likely a typo than a group already gone",
			group)
	}

	if detail.Err != nil {
		return Output{}, fmt.Errorf("consumer group %q: %w", group, detail.Err)
	}

	if len(detail.Members) > 0 {
		return Output{}, fmt.Errorf(
			"refusing to delete group %q: it has %d active member(s), so it is not abandoned. Stop its consumers first if it really is to be removed",
			group, len(detail.Members))
	}

	commits, err := admin.FetchOffsets(ctx, group)
	if err != nil {
		return Output{}, fmt.Errorf("fetch committed offsets for group %q: %w", group, err)
	}

	out := Output{
		Group:       group,
		State:       detail.State,
		Members:     len(detail.Members),
		Topics:      []string{},
		Offsets:     []Offset{},
		WouldDelete: true,
		Warnings:    []string{},
	}

	topics := map[string]bool{}
	commits.Each(func(commit kadm.OffsetResponse) {
		if commit.Err == nil && commit.At >= 0 {
			topics[commit.Topic] = true
		}
	})

	for topic := range topics {
		out.Topics = append(out.Topics, topic)
	}

	sort.Strings(out.Topics)

	var ends kadm.ListedOffsets
	if len(out.Topics) > 0 {
		ends, err = admin.ListEndOffsets(ctx, out.Topics...)
		if err != nil {
			return Output{}, fmt.Errorf("list end offsets: %w", err)
		}
	}

	commits.Each(func(commit kadm.OffsetResponse) {
		if commit.Err != nil || commit.At < 0 {
			return
		}

		offset := Offset{
			Topic:           commit.Topic,
			Partition:       commit.Partition,
			CommittedOffset: commit.At,
			EndOffset:       -1,
		}

		if end, ok := ends.Lookup(commit.Topic, commit.Partition); ok && end.Err == nil {
			offset.EndOffset = end.Offset
			offset.Lag = max(end.Offset-commit.At, 0)
			out.TotalLag += offset.Lag
		}

		out.Offsets = append(out.Offsets, offset)
	})

	sort.Slice(out.Offsets, func(i, j int) bool {
		if out.Offsets[i].Topic != out.Offsets[j].Topic {
			return out.Offsets[i].Topic < out.Offsets[j].Topic
		}

		return out.Offsets[i].Partition < out.Offsets[j].Partition
	})

	if len(out.Offsets) > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"%d committed offset(s) on %d topic(s) would be lost; a consumer that later uses this group id starts from its auto.offset.reset instead",
			len(out.Offsets), len(out.Topics)))
	}

	if out.TotalLag > 0 {
		out.Warnings = append(out.Warnings, fmt.Sprintf(
			"the group has %d unconsumed message(s) of lag; deleting it stops that lag being reported, it does not process the messages",
			out.TotalLag))
	}

	out.Note = "nothing was deleted. Call again with confirm true to delete this group."

	return out, nil
}

func remove(ctx context.Context, admin *kadm.Client, group string) error {
	responses, err := admin.DeleteGroups(ctx, group)
	if err != nil {
		return fmt.Errorf("delete group %q: %w", group, err)
	}

	response, ok := responses[group]
	if !ok {
		return fmt.Errorf("delete group %q: the broker returned no response for it", group)
	}

	if response.Err == nil {
		return nil
	}

	// Authorization failures and a group that came back to life both arrive
	// in the response rather than as a returned error.
	if errors.Is(response.Err, kerr.GroupAuthorizationFailed) {
		return fmt.Errorf(
			"not authorized to delete group %q: the broker refused this request. It needs DELETE permission on the group for the principal this server connects as: %w",
			group, response.Err)
	}

	if errors.Is(response.Err, kerr.NonEmptyGroup) {
		return fmt.Errorf(
			"group %q gained a member before it could be deleted, so it was left in place: %w", group, response.Err)
	}

	return fmt.Errorf("delete group %q: %w", group, response.Err)
}
