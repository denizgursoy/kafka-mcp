package main

import (
	"context"
	"log"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/denizgursoy/kafka-mcp/internal/domain/kafkaclient"
	"github.com/denizgursoy/kafka-mcp/internal/tools/consumerlag"
	"github.com/denizgursoy/kafka-mcp/internal/tools/describetopic"
	"github.com/denizgursoy/kafka-mcp/internal/tools/getmessage"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listconsumergroups"
	"github.com/denizgursoy/kafka-mcp/internal/tools/listtopics"
	"github.com/denizgursoy/kafka-mcp/internal/tools/samplemessages"
	"github.com/denizgursoy/kafka-mcp/internal/tools/searchmessages"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = "localhost:9092"
	}

	kafka, err := kafkaclient.New(broker)
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
	searchmessages.Register(server, kafka.Admin(), kafka.Reader())
	getmessage.Register(server, kafka.Reader())

	log.Println("Kafka MCP server started")

	if err := server.Run(
		context.Background(),
		&mcp.StdioTransport{},
	); err != nil {
		log.Fatal(err)
	}
}
