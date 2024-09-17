package stream

import (
	"encoding/json"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

// TelemetryData represents the CPU and memory usage data structure
type TelemetryData struct {
	Timestamp   time.Time `json:"timestamp"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryTotal uint64    `json:"memory_total"`
	MemoryUsed  uint64    `json:"memory_used"`
}

// DataProducer handles producing stream messages to Kafka
type DataProducer struct {
	Producer *kafka.Producer
}

// NewDataProducer initializes a new Kafka producer using Confluent Kafka
func NewDataProducer(brokers string) (*DataProducer, error) {
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return nil, err
	}

	return &DataProducer{Producer: producer}, nil
}

// CollectTelemetry collects CPU and memory stream data
func CollectTelemetry() (*TelemetryData, error) {
	// Collect CPU percentage (average across all cores)
	cpuPercentages, err := cpu.Percent(0, false)
	if err != nil {
		return nil, err
	}

	// Collect memory usage
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return nil, err
	}

	telemetry := &TelemetryData{
		Timestamp:   time.Now().UTC(),
		CPUUsage:    cpuPercentages[0], // average CPU usage across all cores
		MemoryTotal: vmStat.Total,
		MemoryUsed:  vmStat.Used,
	}

	return telemetry, nil
}

// SendTelemetryMessage sends stream data to the given Kafka topic
func (dp *DataProducer) SendTelemetryMessage(topic string) error {
	telemetryData, err := CollectTelemetry()
	if err != nil {
		return err
	}

	// Serialize stream data to JSON
	messageBytes, err := json.Marshal(telemetryData)
	if err != nil {
		return err
	}

	// Prepare Kafka message
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          messageBytes,
	}

	// Send the message
	err = dp.Producer.Produce(msg, nil)
	if err != nil {
		return err
	}

	// Wait for delivery report to ensure the message was sent
	e := <-dp.Producer.Events()
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}

	log.Printf("Telemetry data sent to topic %s [partition: %d, offset: %d]\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	return nil
}

// Close closes the producer
func (dp *DataProducer) Close() {
	dp.Producer.Close()
}
