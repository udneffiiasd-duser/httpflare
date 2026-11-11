package main

import (
	"flag"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "https://example.com", "целевой адрес")
	conc := flag.Int("c", 200, "число воркеров")
	dur := flag.Duration("d", 30*time.Second, "длительность")
	rate := flag.Int("rate", 0, "максимум запросов в секунду (0 — без лимита)")
	flag.Parse()

	client := &http.Client{Timeout: 15 * time.Second}
	var total, ok, fail int64
	lat := make([]int64, 0, 1<<20)
	var mu sync.Mutex

	deadline := time.Now().Add(*dur)
	var tick <-chan time.Time
	if *rate > 0 {
		t := time.NewTicker(time.Second / time.Duration(*rate))
		defer t.Stop()
		tick = t.C
	}

	var wg sync.WaitGroup
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				if tick != nil {
					<-tick
				}
				start := time.Now()
				resp, err := client.Get(*url)
				d := time.Since(start).Microseconds()
				atomic.AddInt64(&total, 1)
				if err != nil {
					atomic.AddInt64(&fail, 1)
					continue
				}
				resp.Body.Close()
				atomic.AddInt64(&ok, 1)
				mu.Lock()
				lat = append(lat, d)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	p := func(q float64) int64 {
		if len(lat) == 0 {
			return 0
		}
		return lat[int(float64(len(lat)-1)*q)]
	}
	fmt.Printf("всего: %d  успех: %d  ошибок: %d\n", total, ok, fail)
	fmt.Printf("RPS: %.0f\n", float64(total)/dur.Seconds())
	fmt.Printf("p50: %dмкс  p95: %dмкс  p99: %dмкс\n", p(0.50), p(0.95), p(0.99))
}
