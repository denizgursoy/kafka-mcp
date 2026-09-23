// Package listclusters implements the list_clusters MCP tool.
package listclusters

import (
	"context"
	"fmt"
	"sync"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
)

// Input is the argument set accepted by the list_clusters tool. It takes no
// arguments: there is only one roster to report.
type Input struct{}

// Cluster is one entry in the roster.
//
// It carries the name, whether the brokers answer, and whether writes are
// allowed, and nothing else. This tool is reachable from every cluster's
// endpoint, so a session bound to one cluster can see that the others exist.
// It must not also learn how to reach them, which is why no broker address,
// mechanism or principal appears here.
type Cluster struct {
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
	ReadOnly  bool   `json:"read_only"`
}

// Output is the result returned by the list_clusters tool.
type Output struct {
	Clusters []Cluster `json:"clusters"`
	Count    int       `json:"count"`
}

const description = `
List the Kafka clusters this server serves, with whether each is reachable and
whether it accepts writes. Connectivity is checked at call time. Use returned
names as copy_message destination_cluster values. Broker and credential details
are not exposed.
`

// Register adds the list_clusters tool to the MCP server.
//
// It is registered on every cluster's server, since a caller on one endpoint
// needs the whole roster to compose a cross-cluster copy.
func Register(server *mcp.Server, clusters *kafkaclient.Registry) {
	mcp.AddTool(
		server,
		&mcp.Tool{
			Name:        "list_clusters",
			Description: description,
		},
		func(
			ctx context.Context,
			req *mcp.CallToolRequest,
			input Input,
		) (*mcp.CallToolResult, Output, error) {

			out, err := Run(ctx, clusters)
			if err != nil {
				return nil, Output{}, fmt.Errorf("list clusters: %w", err)
			}

			return nil, out, nil
		},
	)
}

// Run reports every configured cluster.
func Run(ctx context.Context, clusters *kafkaclient.Registry) (Output, error) {
	names := clusters.Names()

	out := Output{
		Clusters: make([]Cluster, len(names)),
		Count:    len(names),
	}

	// Each check waits on a broker that may be unreachable, so they run
	// together: one dead cluster must not add its timeout to every other
	// cluster's answer.
	var wait sync.WaitGroup

	for i, name := range names {
		client := clusters.Get(name)
		if client == nil {
			continue
		}

		out.Clusters[i] = Cluster{
			Name:     name,
			ReadOnly: clusters.ReadOnly(name),
		}

		wait.Add(1)

		go func() {
			defer wait.Done()

			// Writing to a distinct index needs no lock, and the slice was
			// sized before any goroutine started.
			out.Clusters[i].Connected = client.Connected(ctx)
		}()
	}

	wait.Wait()

	return out, nil
}
