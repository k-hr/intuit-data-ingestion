package stream

import (
	"encoding/json"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
	"time"
)

// TelemetryData represents the CPU and memory usage data structure
//type TelemetryData struct {
//	Timestamp   time.Time `json:"timestamp"`
//	CPUUsage    float64   `json:"cpu_usage"`
//	MemoryTotal uint64    `json:"memory_total"`
//	MemoryUsed  uint64    `json:"memory_used"`
//}

// DataConsumer handles consuming messages from Kafka using Confluent Kafka
type DataConsumer struct {
	Consumer *kafka.Consumer
}

// NewDataConsumer initializes a new Kafka consumer using Confluent Kafka
func NewDataConsumer(brokers string, groupID string) (*DataConsumer, error) {
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest", // start from the earliest message
	})
	if err != nil {
		return nil, err
	}

	return &DataConsumer{Consumer: consumer}, nil
}

// ConsumeMessages consumes messages from the given topic and processes them
func (dc *DataConsumer) ConsumeMessages(topic string) error {
	// Subscribe to the topic
	err := dc.Consumer.Subscribe(topic, nil)
	if err != nil {
		return err
	}

	// Continuously read messages
	for {
		msg, err := dc.Consumer.ReadMessage(-1)
		if err != nil {
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			continue
		}

		// Deserialize the message
		var telemetry TelemetryData
		err = json.Unmarshal(msg.Value, &telemetry)
		if err != nil {
			log.Printf("Failed to unmarshal telemetry data: %v", err)
			continue
		}

		// Process the stream data
		dc.processTelemetry(telemetry)
	}
}

// processTelemetry processes the consumed stream data (placeholder for real processing logic)
func (dc *DataConsumer) processTelemetry(telemetry TelemetryData) {
	log.Printf("Telemetry data received at %v:\nCPU Usage: %.2f%%\nMemory Used: %d/%d bytes\n",
		telemetry.Timestamp.UTC().Format(time.RFC3339), telemetry.CPUUsage, telemetry.MemoryUsed, telemetry.MemoryTotal)
}

// Close closes the consumer
func (dc *DataConsumer) Close() {
	dc.Consumer.Close()
}
