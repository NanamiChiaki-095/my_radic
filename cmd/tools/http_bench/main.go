package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	status int
	err    error
	lat    time.Duration
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	pos := (p / 100.0) * float64(len(sorted)-1)
	base := int(math.Floor(pos))
	frac := pos - float64(base)
	if base+1 >= len(sorted) {
		return sorted[base]
	}
	return sorted[base] + frac*(sorted[base+1]-sorted[base])
}

func main() {
	url := flag.String("url", "http://127.0.0.1:8080/healthz", "target url")
	total := flag.Int("n", 2000, "total requests")
	concurrency := flag.Int("c", 50, "concurrency")
	timeout := flag.Duration("timeout", 5*time.Second, "request timeout")
	flag.Parse()

	if *total <= 0 || *concurrency <= 0 {
		fmt.Println("invalid args: n and c must be > 0")
		os.Exit(2)
	}

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			MaxConnsPerHost:     *concurrency * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	jobs := make(chan struct{}, *total)
	results := make(chan result, *total)

	var started int64
	startWall := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				atomic.AddInt64(&started, 1)
				reqStart := time.Now()
				resp, err := client.Get(*url)
				if err != nil {
					results <- result{err: err, lat: time.Since(reqStart)}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				results <- result{status: resp.StatusCode, lat: time.Since(reqStart)}
			}
		}()
	}

	for i := 0; i < *total; i++ {
		jobs <- struct{}{}
	}
	close(jobs)
	wg.Wait()
	close(results)

	duration := time.Since(startWall)
	latencies := make([]float64, 0, *total)
	statuses := map[int]int{}
	var errs int
	var sumLatency float64

	for r := range results {
		latMS := float64(r.lat.Microseconds()) / 1000.0
		latencies = append(latencies, latMS)
		sumLatency += latMS
		if r.err != nil {
			errs++
			continue
		}
		statuses[r.status]++
	}

	sort.Float64s(latencies)

	avg := 0.0
	if len(latencies) > 0 {
		avg = sumLatency / float64(len(latencies))
	}

	fmt.Printf("target=%s\n", *url)
	fmt.Printf("requests=%d concurrency=%d started=%d\n", *total, *concurrency, started)
	fmt.Printf("duration_sec=%.3f rps=%.2f\n", duration.Seconds(), float64(*total)/duration.Seconds())
	fmt.Printf("latency_ms avg=%.2f p50=%.2f p95=%.2f p99=%.2f max=%.2f\n",
		avg,
		percentile(latencies, 50),
		percentile(latencies, 95),
		percentile(latencies, 99),
		percentile(latencies, 100),
	)
	fmt.Printf("errors=%d\n", errs)
	fmt.Printf("status_counts=%v\n", statuses)
}
