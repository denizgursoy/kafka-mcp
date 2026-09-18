package main

import (
	"context"
	"log"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/config"
	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools/addpartitions"
	"github.com/denizgursoy/kafka-mcp/internal/tools/commitoffset"
	"github.com/denizgursoy/kafka-mcp/internal/tools/consumerlag"
	"github.com/denizgursoy/kafka-mcp/internal/tools/copymessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/describetopic"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getmessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listconsumergroups"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listtopics"
	"github.com/denizgursoy/kafka-mcp/internal/tools/samplemessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/serverconfig"
)

func main() {
	cfg, err := config.LoadDefault()
	if err != nil {
		log.Fatal(err)
	}

	kafka, err := kafkaclient.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	defer kafka.Close()

	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "kafka-debugger",
			Version: "1.0.0",
		},
		nil,
	)

	// Every tool registers itself: one call per tool, no Kafka logic and no
	// tool schema here.
	listtopics.Register(server, kafka.Admin())
	listconsumergroups.Register(server, kafka.Admin())
	consumerlag.Register(server, kafka.Admin())
	describetopic.Register(server, kafka.Admin(), kafka.Reader())
	samplemessages.Register(server, kafka.Admin(), kafka.Reader())
	searchmessages.Register(server, kafka.Admin(), kafka.Reader(), cfg.OutputDir)
	getmessage.Register(server, kafka.Reader())
	addpartitions.Register(server, kafka, kafka.Reader())
	commitoffset.Register(server, kafka)
	copymessage.Register(server, kafka, kafka.Reader())

	// server_config reports what this server exposes, and the MCP server
	// offers no way to read that back, so the names are listed here beside
	// the registrations they describe.
	serverconfig.Register(server, kafka, []string{
		"add_partitions",
		"commit_offset",
		"consumer_lag",
		"copy_message",
		"describe_topic",
		"get_message",
		"list_consumer_groups",
		"list_topics",
		"sample_messages",
		"search_messages",
		"server_config",
	})

	log.Printf("Kafka MCP server started: brokers %v, read_only %t",
		cfg.Brokers, cfg.ReadOnly)

	if err := server.Run(
		context.Background(),
		&mcp.StdioTransport{},
	); err != nil {
		log.Fatal(err)
	}
}
