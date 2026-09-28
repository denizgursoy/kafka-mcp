// Package audit records which tool a session called, on which cluster, and
// whether it changed anything.
//
// It lives in internal/domain rather than in a tool package because it belongs
// to no single tool: one middleware wraps every tool an endpoint exposes, so a
// tool added later is audited without anyone remembering to add a line to it.
//
// What it deliberately does not record is message content. produce_message and
// copy_message carry arbitrary payloads, and an audit log is usually readable by
// more people than the data it describes, so copying keys, values or headers
// into it would turn the trail into a second, less protected copy of the topic.
// Target names are recorded; payload is not.
package audit

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
)

// maxTargets bounds how many item targets one record names. A batch may carry a
// hundred, and a log line that long is skipped by the person it was written
// for, so the rest are reported as a count instead.
const maxTargets = 10

// mutating is every tool that can change a cluster. They are recorded at info
// and everything else at debug: reads happen constantly and change nothing, so
// logging them at the same level would bury the writes among them.
//
// A tool missing from this set is audited as a read. That is the safe default
// for a new read tool and a visible one for a new write tool, since its records
// simply appear at debug rather than not at all.
var mutating = map[string]bool{
	"add_partitions":  true,
	"commit_offset":   true,
	"copy_message":    true,
	"create_topic":    true,
	"delete_topic":    true,
	"produce_message": true,
}

// Mutates reports whether a tool changes the cluster, and so is recorded at
// info rather than debug.
//
// It is exported so the registration tests can assert that every tool a
// read-only endpoint withholds is audited as a write. Those two lists are
// maintained in different packages, and a tool added to one and missed in the
// other is silently absent from a deployment's audit trail.
func Mutates(tool string) bool {
	return mutating[tool]
}

// item is the target-naming half of one batch entry.
//
// The payload fields of a real item — key, value, headers — are deliberately
// absent, so content cannot reach a log even by mistake: there is nowhere for
// it to be unmarshalled into.
type item struct {
	Topic              string `json:"topic"`
	SourceTopic        string `json:"source_topic"`
	DestinationTopic   string `json:"destination_topic"`
	DestinationCluster string `json:"destination_cluster"`
	Group              string `json:"group"`
	Partition          *int32 `json:"partition"`
	SourcePartition    *int32 `json:"source_partition"`
	Offset             *int64 `json:"offset"`
}

// arguments is the part of a tool call worth recording: how large the batch was,
// whether it was approved, and what it pointed at.
type arguments struct {
	Confirm bool   `json:"confirm"`
	Items   []item `json:"items"`
}

// Principal names the identity the broker sees for a cluster, which is the only
// identity this server can prove: Kafka enforces its ACLs against it.
//
// It is not a person. Everyone reaching the same endpoint shares it, so a record
// naming it answers "which credential acted" and never "who acted".
func Principal(cluster *config.Cluster) string {
	if cluster == nil {
		return ""
	}

	// The first configured identity is the one franz-go prefers, so it is the
	// principal a broker is most likely to have recorded for the call.
	if options := cluster.AuthenticationOptions(); len(options) > 0 && options[0].User != "" {
		return options[0].User
	}

	return "anonymous"
}

// Middleware returns an MCP receiving middleware that records every tool call
// made through it.
//
// The endpoint and principal are fixed at registration because a tool is bound
// to one cluster there, so no caller can make a record name a cluster it did
// not touch.
func Middleware(
	logger *slog.Logger,
	endpoint *config.Endpoint,
	principal string,
) mcp.Middleware {

	return func(next mcp.MethodHandler) mcp.MethodHandler {
		return func(
			ctx context.Context,
			method string,
			request mcp.Request,
		) (mcp.Result, error) {

			call, isCall := request.(*mcp.CallToolRequest)

			// Only tool calls are audited. initialize and tools/list happen on
			// every connection and change nothing, so recording them would
			// drown the calls that matter.
			if !isCall || call.Params == nil {
				return next(ctx, method, request)
			}

			started := time.Now()

			result, err := next(ctx, method, request)

			log(logger, endpoint, principal, call, result, err, time.Since(started))

			return result, err
		}
	}
}

func log(
	logger *slog.Logger,
	endpoint *config.Endpoint,
	principal string,
	call *mcp.CallToolRequest,
	result mcp.Result,
	err error,
	took time.Duration,
) {

	tool := call.Params.Name

	attrs := []any{
		slog.String("tool", tool),
		slog.String("outcome", outcome(result, err)),
		slog.Int64("duration_ms", took.Milliseconds()),
	}

	if endpoint != nil {
		attrs = append(attrs,
			slog.String("endpoint", endpoint.Name),
			slog.String("cluster", endpoint.Cluster),
			slog.Bool("read_only", endpoint.ReadOnly),
		)
	}

	if principal != "" {
		attrs = append(attrs, slog.String("principal", principal))
	}

	attrs = append(attrs, identity(call)...)
	attrs = append(attrs, describeArguments(call.Params.Arguments)...)

	level := slog.LevelDebug
	if mutating[tool] {
		level = slog.LevelInfo
	}

	// The context is not passed on purpose: it may already be cancelled when a
	// call fails or a client disconnects, and an audit record that disappears
	// exactly when something went wrong is worse than no audit at all.
	logger.LogAttrs(context.Background(), level, "tool call", toAttrs(attrs)...)
}

// identity records who made the call, as far as the server can honestly tell.
//
// None of these is a person. The session id groups one investigation, the
// client is the program that connected, and the user appears only when an
// inbound token verifier established it, which this server does not install
// today. Each is omitted when absent, so a reader can tell "not reported" from
// "reported as blank".
func identity(call *mcp.CallToolRequest) []any {
	attrs := make([]any, 0, 4)

	// The session is read through the concrete type rather than the Session
	// interface: a *ServerSession carried in a non-nil interface may still be a
	// nil pointer, and ServerSession.ID dereferences its connection without a
	// nil-receiver guard. Auditing must never be the thing that panics a call.
	if session := call.Session; session != nil {
		if id := session.ID(); id != "" {
			attrs = append(attrs, slog.String("session", id))
		}
	}

	if info := call.ClientInfo(); info != nil {
		if info.Name != "" {
			attrs = append(attrs, slog.String("client", info.Name))
		}

		if info.Version != "" {
			attrs = append(attrs, slog.String("client_version", info.Version))
		}
	}

	extra := call.GetExtra()
	if extra == nil {
		return attrs
	}

	// A verified bearer identity is the only per-person attribution available.
	if extra.TokenInfo != nil && extra.TokenInfo.UserID != "" {
		attrs = append(attrs, slog.String("user", extra.TokenInfo.UserID))
	}

	// The HTTP layer already assigns a request id, and carrying it here is what
	// joins an audit record to that access log.
	if extra.Header != nil {
		if id := extra.Header.Get("X-Request-Id"); id != "" {
			attrs = append(attrs, slog.String("request_id", id))
		}
	}

	return attrs
}

// describeArguments reports the shape of the call without its payload.
//
// Unparseable arguments are still worth a record: a malformed call is exactly
// the kind of thing an audit should show, so the failure is reported rather
// than dropping the line.
func describeArguments(raw json.RawMessage) []any {
	if len(raw) == 0 {
		return nil
	}

	var parsed arguments

	if err := json.Unmarshal(raw, &parsed); err != nil {
		return []any{slog.Bool("arguments_parsed", false)}
	}

	attrs := []any{
		slog.Bool("confirm", parsed.Confirm),
		slog.Int("item_count", len(parsed.Items)),
	}

	if targets := targets(parsed); targets != "" {
		attrs = append(attrs, slog.String("targets", targets))
	}

	return attrs
}

// targets names what each item pointed at, so a reader learns which topic was
// touched rather than only that some topic was.
func targets(parsed arguments) string {
	named := make([]string, 0, len(parsed.Items))

	for index, item := range parsed.Items {
		if index == maxTargets {
			named = append(named,
				"and "+strconv.Itoa(len(parsed.Items)-maxTargets)+" more")

			break
		}

		if described := describeItem(item); described != "" {
			named = append(named, described)
		}
	}

	return strings.Join(named, ", ")
}

func describeItem(item item) string {
	parts := make([]string, 0, 4)

	if item.Topic != "" {
		parts = append(parts, item.Topic)
	}

	if item.SourceTopic != "" {
		parts = append(parts, item.SourceTopic)
	}

	// A copy names two topics and possibly two clusters, and the destination is
	// the half that was written to, so it is the half an audit cannot omit.
	if item.DestinationTopic != "" {
		destination := "->" + item.DestinationTopic
		if item.DestinationCluster != "" {
			destination = "->" + item.DestinationCluster + "/" + item.DestinationTopic
		}

		parts = append(parts, destination)
	}

	if item.Group != "" {
		parts = append(parts, "group="+item.Group)
	}

	partition := item.Partition
	if partition == nil {
		partition = item.SourcePartition
	}

	if partition != nil {
		parts = append(parts, "p"+strconv.Itoa(int(*partition)))
	}

	if item.Offset != nil {
		parts = append(parts, "offset="+strconv.FormatInt(*item.Offset, 10))
	}

	return strings.Join(parts, " ")
}

// outcome separates the three ways a call can end, because they point at
// different faults: the tool refused, the call never reached the tool, or it
// worked.
func outcome(result mcp.Result, err error) string {
	if err != nil {
		return "error"
	}

	if call, ok := result.(*mcp.CallToolResult); ok && call != nil && call.IsError {
		return "tool_error"
	}

	return "ok"
}

func toAttrs(values []any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(values))

	for _, value := range values {
		if attr, ok := value.(slog.Attr); ok {
			attrs = append(attrs, attr)
		}
	}

	return attrs
}
