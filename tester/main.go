package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type result struct {
	successes int
	errors    int
}

func main() {
	k := koanf.New(".")
	if err := k.Load(file.Provider("config.json"), json.Parser()); err != nil {
		log.Fatalf("error loading config: %v", err)
	}

	var cfg Config
	if err := k.UnmarshalWithConf("", &cfg, koanf.UnmarshalConf{}); err != nil {
		log.Fatalf("error unmarshalling config: %v", err)
	}

	var stats syncMap[string, result]

	var wg sync.WaitGroup

	for _, app := range cfg.Apps {
		fmt.Printf("[%s] starting tests\n", app.Name)
		ports := app.Ports
		n := rand.Intn(5) + 1
		for i := 0; i < n; i++ {
			for _, p := range ports {
				wg.Add(1)
				go func(appName string, p int) {
					defer wg.Done()
					payload := fmt.Sprintf("%s %d", appName, p)

					conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", p))
					if err != nil {
						IncrementErrors(&stats, payload)
						log.Printf("[%s] ERROR connecting to port %d", appName, p)
						return
					}
					defer conn.Close()

					if _, err := io.Copy(conn, strings.NewReader(payload)); err != nil {
						IncrementErrors(&stats, payload)
						log.Printf("[%s] ERROR sending payload to port %d: %v", appName, p, err)
						return
					}

					buf := &bytes.Buffer{}
					conn.SetReadDeadline(time.Now().Add(30 * time.Second))
					if _, err := io.Copy(buf, conn); err != nil {
						IncrementErrors(&stats, payload)
						log.Printf("[%s] ERROR receiving response from port %d: %v", appName, p, err)
						return
					}

					res := buf.String()
					status := "ERROR"
					if res == payload {
						status = "SUCCESS"
						IncrementSuccesses(&stats, payload)
					} else {
						IncrementErrors(&stats, payload)
					}

					log.Printf("[%s] %s receiving response from port %d: payload=%q response=%q",
						appName, status, p, payload, res,
					)
				}(app.Name, p)
			}

			rand.Shuffle(len(ports), func(i, j int) {
				ports[i], ports[j] = ports[j], ports[i]
			})
		}

		fmt.Printf("[%s] %d iterations initiated\n", app.Name, n)
	}

	wg.Wait()

	stats.Range(func(k string, v result) bool {
		fmt.Printf("[%s] -> successes=%d errors=%d\n", k, v.successes, v.errors)
		return true
	})
}

func IncrementSuccesses[K any](m *syncMap[K, result], k K) {
	r, _ := m.Load(k)
	r.successes += 1
	m.Store(k, r)
}

func IncrementErrors[K any](m *syncMap[K, result], k K) {
	r, _ := m.Load(k)
	r.errors += 1
	m.Store(k, r)
}

type syncMap[K, V any] struct {
	m sync.Map
}

func (m *syncMap[K, V]) Range(f func(K, V) bool) {
	m.m.Range(func(kA, vA any) bool {
		k, ok := kA.(K)
		if !ok {
			return true
		}

		v, ok := vA.(V)
		if !ok {
			return true
		}

		return f(k, v)
	})
}

func (m *syncMap[K, V]) Store(k K, v V) {
	m.m.Store(k, v)
}

func (m *syncMap[K, V]) Load(k K) (V, bool) {
	vA, _ := m.m.Load(k)

	v, ok := vA.(V)
	if !ok {
		var d V
		return d, ok
	}

	return v, ok
}

func (m *syncMap[K, V]) LoadOrStore(k K, d V) (V, bool) {
	v, _ := m.m.LoadOrStore(k, d)
	vv, ok := v.(V)
	if !ok {
		return d, ok
	}

	return vv, ok
}

type Config struct {
	Apps []GroupConfig `koanf:"Apps"`
}

type GroupConfig struct {
	Name    string   `koanf:"Name"`
	Ports   []int    `koanf:"Ports"`
	Targets []string `koanf:"Targets"`
}
