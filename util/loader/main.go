package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type AddDocRequest struct {
	Title   string `json:"title"`
	Content string `json:"content"`
	Url     string `json:"url"`
}

var (
	targetURL   = flag.String("url", "http://localhost:8080/add_doc", "Target API Gateway URL")
	concurrency = flag.Int("c", 100, "Number of concurrent workers")
	totalDocs   = flag.Int("n", 50000, "Total number of documents to add")
)

var words = []string{"Golang", "分布式", "搜索引擎", "微服务", "Kafka", "MySQL", "Redis", "SkipList", "一致性", "高可用", "面试", "后端", "性能优化"}

func generateRandomText(length int) string {
	res := ""
	for i := 0; i < length; i++ {
		res += words[rand.Intn(len(words))] + " "
	}
	return res
}

func main() {
	flag.Parse()

	fmt.Printf("Starting STRESS TEST: target=%s, concurrency=%d, total=%d\n", *targetURL, *concurrency, *totalDocs)

	jobs := make(chan AddDocRequest, *totalDocs)
	var wg sync.WaitGroup

	// 1. 生成任务
	go func() {
		for i := 0; i < *totalDocs; i++ {
			jobs <- AddDocRequest{
				Title:   fmt.Sprintf("%s 详解 - %d", words[rand.Intn(len(words))], i),
				Content: generateRandomText(20 + rand.Intn(50)),
				Url:     fmt.Sprintf("http://example.com/article/%d", i),
			}
		}
		close(jobs)
	}()

	start := time.Now()
	successCount := 0
	var mu sync.Mutex

	// 2. 启动 Workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// 增加连接池配置，避免高并发下端口耗尽
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns = 100
			t.MaxConnsPerHost = 100
			t.MaxIdleConnsPerHost = 100
			client := &http.Client{
				Timeout:   10 * time.Second,
				Transport: t,
			}

			for req := range jobs {
				body, _ := json.Marshal(req)
				resp, err := client.Post(*targetURL, "application/json", bytes.NewBuffer(body))
				if err != nil {
					fmt.Printf("Worker %d: Request error: %v\n", id, err)
					continue
				}
				
				// 必须读取 Body 并关闭，否则会导致连接泄漏
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()

				if resp.StatusCode == http.StatusOK {
					mu.Lock()
					successCount++
					mu.Unlock()
				} else {
					fmt.Printf("Worker %d: Failed with status %d\n", id, resp.StatusCode)
				}
			}
		}(i)
	}

	// 3. 等待完成
	wg.Wait()
	duration := time.Since(start)

	fmt.Println("\n--- Stress Test Complete ---")
	fmt.Printf("Time taken: %v\n", duration)
	fmt.Printf("Successful uploads: %d\n", successCount)
	fmt.Printf("QPS (TPS): %.2f docs/sec\n", float64(successCount)/duration.Seconds())
}
