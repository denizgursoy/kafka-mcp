package main

import (
	"context"
	"log"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	kafkaclient "github.com/denizgursoy/kafka-mcp/internal/kafka"
	"github.com/denizgursoy/kafka-mcp/internal/kafka/listtopics"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")

	if broker == "" {
		broker = "localhost:9092"
	}

	kafka, err := kafkaclient.NewClient(broker)
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

	log.Println("Kafka MCP server started")

	if err := server.Run(
		context.Background(),
		&mcp.StdioTransport{},
	); err != nil {
		log.Fatal(err)
	}
}
