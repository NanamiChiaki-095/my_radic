package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

func main() {
	// 准备更真实的技术博文数据
	docs := []map[string]interface{}{
		{
			"title":   "Introduction to Golang Concurrency",
			"content": "Goroutines and channels are the core primitives of Go's concurrency model. They allow highly efficient parallel processing with low memory footprint.",
			"url":     "https://go.dev/doc/concurrency",
		},
		{
			"title":   "Mastering Kafka for Real-time Data Streams",
			"content": "Apache Kafka is a distributed event store and stream-processing platform. It provides high throughput and low latency for handling real-time data feeds.",
			"url":     "https://kafka.apache.org/documentation/",
		},
		{
			"title":   "Redis as a Distributed Cache",
			"content": "Redis is an open-source, in-memory data structure store, used as a database, cache, and message broker. It supports various data types like strings, hashes, and sets.",
			"url":     "https://redis.io/docs/",
		},
		{
			"title":   "BadgerDB: The LSM Tree Powerhouse",
			"content": "BadgerDB is an embeddable, persistent, simple and fast key-value (KV) database written in pure Go. It is optimized for SSDs using an LSM tree architecture.",
			"url":     "https://github.com/dgraph-io/badger",
		},
	}

	for _, doc := range docs {
		jsonData, _ := json.Marshal(doc)
		resp, err := http.Post("http://localhost:8080/add_doc", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			fmt.Printf("Error sending doc '%s': %v\n", doc["title"], err)
			continue
		}
		
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()
		
		fmt.Printf("Sent: %-40s | Status: %s | ID: %v\n", doc["title"], resp.Status, result["data"])
	}

	fmt.Println("\nAll docs sent to MySQL. Relay will push them to Kafka shortly.")
	fmt.Println("Waiting 3 seconds for indexer to process...")
	time.Sleep(3 * time.Second)
}