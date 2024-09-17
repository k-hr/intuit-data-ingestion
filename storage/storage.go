package storage

import (
	"log"
	"sync"
	"time"
)

// TelemetryData represents telemetry data for CPU and memory usage
type TelemetryData struct {
	Timestamp   time.Time `json:"timestamp"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryTotal uint64    `json:"memory_total"`
	MemoryUsed  uint64    `json:"memory_used"`
}

// DistributedStorage represents a simple distributed storage system with high availability
type DistributedStorage struct {
	nodes   [][]TelemetryData
	nodeNum int
	mutex   sync.RWMutex
}

// NewDistributedStorage initializes a new distributed storage system
func NewDistributedStorage(nodeCount int) *DistributedStorage {
	storage := &DistributedStorage{
		nodeNum: nodeCount,
		nodes:   make([][]TelemetryData, nodeCount),
	}
	return storage
}

// Write stores telemetry data on a random node
func (ds *DistributedStorage) Write(telemetry TelemetryData) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// Simulate writing to a random node
	nodeIndex := int(time.Now().UnixNano()) % ds.nodeNum
	ds.nodes[nodeIndex] = append(ds.nodes[nodeIndex], telemetry)
	log.Printf("Telemetry data written to node %d", nodeIndex)
}

// ReplicateData replicates telemetry data across all nodes for redundancy
func (ds *DistributedStorage) ReplicateData() {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	// Simple replication: copy data from one node to all others
	for i := 1; i < ds.nodeNum; i++ {
		ds.nodes[i] = ds.nodes[0] // Simulating replication from node 0
		log.Printf("Replicated data from node 0 to node %d", i)
	}
}

// ReadAll returns all telemetry data stored in all nodes
func (ds *DistributedStorage) ReadAll() []TelemetryData {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	var allData []TelemetryData
	for _, node := range ds.nodes {
		allData = append(allData, node...)
	}
	return allData
}
