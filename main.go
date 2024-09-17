package main

import (
	"log"
	"os"
	"os/signal"
	"time"

	"intuit-data-ingestion/query"
	"intuit-data-ingestion/storage"
	"intuit-data-ingestion/stream"
)

func main() {
	// Initialize distributed storage with 3 nodes
	storageSystem := storage.NewDistributedStorage(3)

	// Initialize query engine
	queryEngine := query.NewQueryEngine()

	// Kafka settings
	brokers := "localhost:9092"
	topic := "telemetry_topic"
	groupID := "consumer_group_1"

	// Initialize Kafka producer using Confluent Kafka
	dataProducer, err := stream.NewDataProducer(brokers)
	if err != nil {
		log.Fatalf("Failed to initialize producer: %v", err)
	}
	defer dataProducer.Close()

	// Simulate sending telemetry data to Kafka every 5 seconds
	go func() {
		for {
			err := dataProducer.SendTelemetryMessage(topic)
			if err != nil {
				log.Fatalf("Failed to send telemetry message: %v", err)
			}
			time.Sleep(5 * time.Second)
		}
	}()

	// Initialize Kafka consumer using Confluent Kafka
	dataConsumer, err := stream.NewDataConsumer(brokers, groupID)
	if err != nil {
		log.Fatalf("Failed to initialize consumer: %v", err)
	}
	defer dataConsumer.Close()

	// Start consuming telemetry messages in a separate goroutine
	go func() {
		err := dataConsumer.ConsumeMessages(topic)
		if err != nil {
			log.Fatalf("Failed to consume messages: %v", err)
		}
	}()

	// Periodically replicate data across storage nodes
	go func() {
		for {
			time.Sleep(10 * time.Second)
			storageSystem.ReplicateData()
		}
	}()

	// Querying telemetry data periodically
	go func() {
		for {
			time.Sleep(15 * time.Second)

			// Prepare query request (e.g., raw query for the last 30 minutes)
			queryRequest := query.QueryRequest{
				QueryType:  query.RawQuery,
				StartTime:  time.Now().Add(-30 * time.Minute),
				EndTime:    time.Now(),
				MetricName: "telemetry",
			}

			// Execute query
			queryResponse, err := queryEngine.Execute(queryRequest)
			if err != nil {
				log.Printf("Query failed: %v", err)
				continue
			}

			// Log query results
			log.Printf("Telemetry Query Results: %+v", queryResponse.Data)
		}
	}()

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println("Shutting down...")
}
