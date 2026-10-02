// Package listacls implements the list_acls MCP tool.
package listacls

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/twmb/franz-go/pkg/kadm"
)

// Input is the argument set accepted by the list_acls tool.
//
// It takes no items: one call already lists every matching entry, and a batch
// would only hide that the broker answers each filter separately.
type Input struct {
	Principal    string `json:"principal,omitempty" jsonschema:"Optional principal to list ACLs for, including its type, such as User:payments. Matched exactly and case-sensitively. Omit for every principal."`
	ResourceType string `json:"resource_type,omitempty" jsonschema:"Optional resource type: topic, group, cluster, transactional_id or delegation_token. Case-insensitive. Omit for every type. Required when resource_name is given."`
	ResourceName string `json:"resource_name,omitempty" jsonschema:"Optional resource name, such as a topic or group. Returns every ACL the broker applies to that name: the exact name, prefixed ACLs whose prefix it starts with, and the * wildcard. Case-sensitive. Needs resource_type."`
}

// ACL is one access control entry.
type ACL struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	ResourceType string `json:"resource_type"`
	ResourceName string `json:"resource_name"`
	PatternType  string `json:"pattern_type"`
	Operation    string `json:"operation"`
	Permission   string `json:"permission"`
}

// Output is the result returned by the list_acls tool.
type Output struct {
	ACLs  []ACL `json:"acls"`
	Count int   `json:"count"`
}

const description = `
List the access control entries (ACLs) on the cluster this endpoint serves.
Each entry has principal, host, resource_type, resource_name, pattern_type
(literal, prefixed), operation (read, write, describe, ...) and permission
(allow or deny). Results are sorted.

Use it when a client fails with TOPIC_AUTHORIZATION_FAILED,
GROUP_AUTHORIZATION_FAILED or similar: filter by the client's principal, or by
the resource it was refused on. Filtering by resource_name returns every ACL
the broker applies to that name, including prefixed and wildcard entries, so
the answer covers what actually decides access. A deny overrides any allow.

All filters are optional and combine. Fails with SECURITY_DISABLED when the
broker has no authorizer, which means ACLs are not enforced at all. Needs
DESCRIBE permission on the cluster.
`

// Register adds the list_acls tool to the MCP server.
func Register(server *mcp.Server, admin *kadm.Client) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "list_acls",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, admin, input)
			if err != nil {
				return nil, Output{}, fmt.Errorf("list acls: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run describes every ACL matching the filters.
func Run(ctx context.Context, admin *kadm.Client, input Input) (Output, error) {
	builder, err := filter(input)
	if err != nil {
		return Output{}, err
	}

	results, err := admin.DescribeACLs(ctx, builder)
	if err != nil {
		return Output{}, fmt.Errorf("describe ACLs: %w", err)
	}

	out := Output{ACLs: []ACL{}}

	// One entry can match more than one filter, for instance when an allow
	// and a deny filter are both sent, so entries are de-duplicated.
	seen := map[ACL]bool{}

	for _, result := range results {
		if result.Err != nil {
			if result.ErrMessage != "" {
				return Output{}, fmt.Errorf("describe ACLs: %w: %s", result.Err, result.ErrMessage)
			}

			return Output{}, fmt.Errorf("describe ACLs: %w", result.Err)
		}

		for _, described := range result.Described {
			acl := ACL{
				Principal:    described.Principal,
				Host:         described.Host,
				ResourceType: words(described.Type.String()),
				ResourceName: described.Name,
				PatternType:  words(described.Pattern.String()),
				Operation:    words(described.Operation.String()),
				Permission:   words(described.Permission.String()),
			}

			if seen[acl] {
				continue
			}

			seen[acl] = true
			out.ACLs = append(out.ACLs, acl)
		}
	}

	sort.Slice(out.ACLs, func(i, j int) bool {
		a, b := out.ACLs[i], out.ACLs[j]

		for _, pair := range [][2]string{
			{a.Principal, b.Principal},
			{a.ResourceType, b.ResourceType},
			{a.ResourceName, b.ResourceName},
			{a.PatternType, b.PatternType},
			{a.Operation, b.Operation},
			{a.Permission, b.Permission},
			{a.Host, b.Host},
		} {
			if pair[0] != pair[1] {
				return pair[0] < pair[1]
			}
		}

		return false
	})

	out.Count = len(out.ACLs)

	return out, nil
}

// filter builds the describe filter. Every field left empty matches anything.
func filter(input Input) (*kadm.ACLBuilder, error) {
	if input.ResourceName != "" && input.ResourceType == "" {
		return nil, fmt.Errorf(
			"resource_name needs resource_type: a topic and a group may share a name, so the name alone does not say which resource is meant")
	}

	builder := kadm.NewACLs().Operations()

	if input.Principal != "" {
		builder.Allow(input.Principal).AllowHosts().Deny(input.Principal).DenyHosts()
	} else {
		builder.Allow().AllowHosts().Deny().DenyHosts()
	}

	// MATCH is the pattern that returns what the broker would apply to one
	// name: the literal entry, covering prefixes and the wildcard. Without a
	// name, ANY returns every entry of the type.
	pattern := kadm.ACLPatternAny
	names := []string{}

	if input.ResourceName != "" {
		pattern = kadm.ACLPatternMatch
		names = append(names, input.ResourceName)
	}

	switch strings.ToLower(input.ResourceType) {
	case "":
		builder.AnyResource()
	case "topic":
		builder.Topics(names...)
	case "group":
		builder.Groups(names...)
	case "cluster":
		builder.Clusters()
	case "transactional_id":
		builder.TransactionalIDs(names...)
	case "delegation_token":
		builder.DelegationTokens(names...)
	default:
		return nil, fmt.Errorf(
			"resource_type must be topic, group, cluster, transactional_id or delegation_token, got %q", input.ResourceType)
	}

	builder.ResourcePatternType(pattern)

	return builder, nil
}

// words renders Kafka's enum names the way the tool's schema documents them.
func words(name string) string {
	return strings.ToLower(name)
}
