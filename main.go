package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"time"

	"intuit-data-ingestion/query"
	"intuit-data-ingestion/storage"
	"intuit-data-ingestion/stream"
)

var (
	storageSystem *storage.DistributedStorage
	queryEngine   *query.QueryEngine
)

func main() {
	// Initialize distributed storage with 3 nodes
	storageSystem = storage.NewDistributedStorage(3)

	// Initialize query engine
	queryEngine = query.NewQueryEngine()

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

	// Start HTTP Server
	http.HandleFunc("/query", queryHandler)
	http.HandleFunc("/health", healthHandler)
	go func() {
		log.Println("Starting server on :8080...")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	// Handle graceful shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Println("Shutting down...")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// queryHandler serves as an endpoint to perform ad-hoc queries on telemetry data
func queryHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters
	metricName := r.URL.Query().Get("metric")
	startTimeStr := r.URL.Query().Get("start")
	endTimeStr := r.URL.Query().Get("end")

	if metricName == "" || startTimeStr == "" || endTimeStr == "" {
		http.Error(w, "Missing query parameters", http.StatusBadRequest)
		return
	}

	// Parse start and end times in UTC
	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		http.Error(w, "Invalid start time format. Use RFC3339 format (e.g., 2024-09-17T12:00:00Z)", http.StatusBadRequest)
		return
	}
	startTime = startTime.UTC()

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		http.Error(w, "Invalid end time format. Use RFC3339 format (e.g., 2024-09-17T13:00:00Z)", http.StatusBadRequest)
		return
	}
	endTime = endTime.UTC()

	// Create query request
	queryRequest := query.QueryRequest{
		QueryType:  query.RawQuery,
		StartTime:  startTime,
		EndTime:    endTime,
		MetricName: metricName,
	}

	// Execute the query
	queryResponse, err := queryEngine.Execute(queryRequest)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Respond with the results
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(queryResponse); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}
}
