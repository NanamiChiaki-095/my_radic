package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

var (
	targetURL   = flag.String("url", "http://localhost:8080/search_http", "Target API Gateway URL")
	concurrency = flag.Int("c", 100, "Number of concurrent workers")
	duration    = flag.Duration("d", 10*time.Second, "Test duration")
)

// 模拟随机搜索词 (强制缓存未命中)
const letters = "abcdefghijklmnopqrstuvwxyz"

func randomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func main() {
	flag.Parse()

	fmt.Printf("Starting COLD/MISS STRESS TEST: target=%s, concurrency=%d, duration=%v\n", *targetURL, *concurrency, *duration)

	var wg sync.WaitGroup
	start := time.Now()
	
	// 统计数据
	var totalReqs int64 = 0
	var successReqs int64 = 0
	var mu sync.Mutex

	// 停止信号
	done := make(chan struct{})
	
	// 启动 Workers
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			// 高并发 HTTP Client 配置
			t := http.DefaultTransport.(*http.Transport).Clone()
			t.MaxIdleConns = 200
			t.MaxConnsPerHost = 200
			t.MaxIdleConnsPerHost = 200
			client := &http.Client{
				Timeout:   3 * time.Second,
				Transport: t,
			}

			for {
				select {
				case <-done:
					return
				default:
					// 生成随机词，保证穿透
					kw := randomString(5) 
					url := fmt.Sprintf("%s?q=%s", *targetURL, kw)

					resp, err := client.Get(url)
					
					mu.Lock()
					totalReqs++;
					if err == nil {
						// 必须读取完 Body
						io.Copy(io.Discard, resp.Body)
						resp.Body.Close()
						if resp.StatusCode == 200 {
							successReqs++
						}
					}
					mu.Unlock()
				}
			}
		}(i)
	}
// ...
	// 等待指定时间
	time.Sleep(*duration)
	close(done)
	wg.Wait()

	elapsed := time.Since(start).Seconds()

	fmt.Println("\n--- Search Test Complete ---")
	fmt.Printf("Time taken: %.2fs\n", elapsed)
	fmt.Printf("Total Requests: %d\n", totalReqs)
	fmt.Printf("Successful: %d\n", successReqs)
	fmt.Printf("Search QPS: %.2f req/sec\n", float64(successReqs)/elapsed)
}
