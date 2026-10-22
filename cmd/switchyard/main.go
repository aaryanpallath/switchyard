// switchyard: an HTTP load balancer with health checks, passive ejection and retries.
//
//	switchyard -listen :8080 -admin 127.0.0.1:8081 -backend http://10.0.0.5:9000 -backend http://10.0.0.6:9000
//
// Traffic arrives on -listen; operators manage backends through the admin API on -admin, which listens on
// localhost by default because it can redirect all traffic.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aaryanpallath/switchyard/internal/admin"
	"github.com/aaryanpallath/switchyard/internal/pool"
	"github.com/aaryanpallath/switchyard/internal/proxy"
)

type list []string

func (l *list) String() string     { return strings.Join(*l, ",") }
func (l *list) Set(v string) error { *l = append(*l, v); return nil }

func main() {
	var backends list
	listen := flag.String("listen", ":8080", "address for proxied traffic")
	adminAddr := flag.String("admin", "127.0.0.1:8081", "address for the admin API")
	strategy := flag.String("strategy", string(pool.RoundRobin), "round_robin or least_connections")
	healthPath := flag.String("health-path", "/healthz", "backend health endpoint")
	interval := flag.Duration("health-interval", 2*time.Second, "time between health checks")
	flag.Var(&backends, "backend", "backend URL (repeatable)")
	flag.Parse()

	cfg := pool.DefaultConfig()
	cfg.Strategy = pool.Strategy(*strategy)
	if cfg.Strategy != pool.RoundRobin && cfg.Strategy != pool.LeastConnections {
		log.Fatalf("unknown strategy %q", *strategy)
	}
	p := pool.New(cfg)
	for _, b := range backends {
		if _, err := p.Add(b); err != nil {
			log.Fatal(err)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go (&pool.Prober{Pool: p, Path: *healthPath, Interval: *interval, Timeout: time.Second}).Run(ctx)

	servers := []*http.Server{
		{Addr: *listen, Handler: proxy.New(p, proxy.DefaultConfig()), ReadHeaderTimeout: 5 * time.Second},
		{Addr: *adminAddr, Handler: admin.Handler(p), ReadHeaderTimeout: 5 * time.Second},
	}
	for _, s := range servers {
		go func() {
			log.Printf("listening on %s", s.Addr)
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Fatal(err)
			}
		}()
	}
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, s := range servers {
		_ = s.Shutdown(shutdown) // finish requests in flight
	}
}
