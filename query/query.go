package query

import (
	"fmt"
	"time"
)

// TelemetryData represents the telemetry data for CPU and memory usage
type TelemetryData struct {
	Timestamp   time.Time `json:"timestamp"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryTotal uint64    `json:"memory_total"`
	MemoryUsed  uint64    `json:"memory_used"`
}

// QueryType defines the type of query being executed
type QueryType string

const (
	RawQuery       QueryType = "raw"
	AggregateQuery QueryType = "aggregate"
)

// QueryRequest represents the structure of a query
type QueryRequest struct {
	QueryType  QueryType `json:"query_type"`
	StartTime  time.Time `json:"start_time"`
	EndTime    time.Time `json:"end_time"`
	MetricName string    `json:"metric_name"`
}

// QueryResponse contains the results of a query
type QueryResponse struct {
	Data []TelemetryData `json:"data"`
}

// QueryEngine handles query execution on stored telemetry data
type QueryEngine struct {
	storage map[string][]TelemetryData
}

// NewQueryEngine initializes a new QueryEngine
func NewQueryEngine() *QueryEngine {
	return &QueryEngine{
		storage: make(map[string][]TelemetryData),
	}
}

// Execute executes a query on the telemetry data
func (qe *QueryEngine) Execute(req QueryRequest) (*QueryResponse, error) {
	data, exists := qe.storage[req.MetricName]
	if !exists {
		return nil, fmt.Errorf("metric %s not found", req.MetricName)
	}

	// Filter data by the time range
	var filteredData []TelemetryData
	for _, entry := range data {
		entryTime := entry.Timestamp.UTC()
		if entryTime.After(req.StartTime) && entryTime.Before(req.EndTime) {
			filteredData = append(filteredData, entry)
		}
	}

	return &QueryResponse{
		Data: filteredData,
	}, nil
}

// AddData adds new telemetry data to the storage for querying
func (qe *QueryEngine) AddData(metricName string, telemetry TelemetryData) {
	qe.storage[metricName] = append(qe.storage[metricName], telemetry)
}
