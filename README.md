# switchyard: A Fault-Tolerant HTTP Load Balancer in Go

An HTTP load balancer and backend registry written in Go with only the standard library. It has a REST admin API,
active health checks, passive ejection of failing backends, retries with per-try timeouts, and round-robin or
least-connections routing. It comes with a **failure-injection experiment** that measures how much each feature
protects clients when backends die, fail intermittently or slow down.

## Results

Five backends serve a request in about 2 ms. Two seconds into each 12-second run, three of them fail:

| Backend | What happens | Can a health check see it? |
|---|---|---|
| dead | the process exits; connections are refused | yes |
| flaky | half of its responses are 503 | **no**: its health endpoint still returns 200 |
| slow | every request takes 1.5 s, including its health check | yes, with a 300 ms probe timeout |

32 clients send requests in a loop through the balancer, each with a 5-second timeout. Every configuration faces
the same scenario on fresh backends. Figures are measured from the moment of failure (`results/chaos.json`):

| Configuration | Requests served | Failed | Median | p99 | Requests/s |
|---|---|---|---|---|---|
| Round robin, nothing else | 1,120 | **29.4%** | 2.3 ms | **1,503 ms** | 112 |
| + active health checks | 111,384 | 16.6% | 2.4 ms | 3.6 ms | 11,138 |
| + passive ejection | 110,587 | 0.02% | 2.4 ms | 3.9 ms | 11,059 |
| + retries (3 tries, 300 ms each) | 126,108 | **0%** | 2.4 ms | 3.7 ms | 12,611 |
| All of it, least connections | 129,957 | 0% | 2.3 ms | 3.7 ms | 12,996 |

A second run (`results/chaos-run2.json`) agreed within a few percent: 29.3%, 16.6%, 0.02%, 0%, 0% failed.

- **A slow backend is worse than a dead one.** Without health checks, throughput fell from about 12,000 to 112
  requests a second. Each client that drew the slow backend waited 1.5 s, so the clients piled up there. The dead
  backend fails fast and costs little time; the slow one consumes the clients.
- **Health checks miss partial failures.** They removed the dead and slow backends but not the flaky one, whose
  health endpoint kept passing. A third of the traffic went to it, and half of that failed: 16.6%, almost exactly
  the expected 1/3 × 1/2.
- **Passive ejection catches what probes can't.** Watching real responses, the balancer ejects a backend after 3
  failures in a row. The 0.02% left over is the 3 requests that re-trigger ejection each time it expires (every 5 s).
- **Retries remove the rest,** but only for idempotent methods. A POST is never retried, since retrying a
  payment could charge it twice; a test checks this.

## The experiment found a real bug

The first run crashed the proxy as soon as the dead backend refused a connection. When a try failed, the retry
loop called a cleanup function that the error path returned as `nil`. Two lessons:
- **Failure paths need tests as much as success paths do.** The fix returns a no-op function, and a regression
  test covers it.
- **The regression test passed even with the bug put back.** Go's HTTP client quietly resends a GET when the
  connection dies before a response, so the second backend answered and the crash was invisible. The test now calls
  the handler directly, and it fails with the bug in place.

## Usage

```bash
go run ./cmd/switchyard -listen :8080 -admin 127.0.0.1:8081 \
    -backend http://10.0.0.5:9000 -backend http://10.0.0.6:9000 -strategy least_connections

curl -X POST localhost:8081/backends -d '{"url": "http://10.0.0.7:9000"}'   # 201 {"id": "b3", ...}
curl localhost:8081/backends        # health, ejection state, in-flight and served counts per backend
curl -X DELETE localhost:8081/backends/b3

go run ./cmd/chaos                  # the experiment above (about a minute)
go test -race ./...
```

| Admin endpoint | |
|---|---|
| `GET /backends` | every backend: up or down, ejected until when, requests in flight, served, failed |
| `POST /backends` `{"url": ...}` | register (422 for an invalid URL, 409 for a duplicate, 400 for unknown fields) |
| `DELETE /backends/{id}` | remove (404 if unknown) |
| `GET /healthz` | the balancer's own health |

**Design notes**
- **The admin API listens on localhost by default.** Anyone who can register a backend can redirect all traffic.
- **Request bodies are buffered (1 MB limit, 413 above it),** so a retry can resend them.
- **Hop-by-hop headers are stripped** in both directions, and `X-Forwarded-For` is set from the connection itself,
  not from a header the client could forge.
- **Health state needs streaks.** A backend goes down after 2 failed probes in a row and comes back after 2 passes,
  so a single blip doesn't flap it.
- **Least connections breaks ties round-robin,** so idle backends share load evenly instead of the first one
  taking everything.
- **Shutdown is graceful:** on SIGTERM, requests in flight finish before exit.

## Tests

`go test -race ./...`, run in CI with the race detector, gofmt, `go vet` and a short version of the experiment:
- **Pool:** URL validation, round robin skipping down and excluded backends, health thresholds needing consecutive
  results, passive ejection and its expiry (with an injected clock), least connections avoiding a busy backend.
- **Proxy:** path, query, body and header forwarding; a refused connection retried elsewhere; a POST never retried;
  a slow backend cut off by the per-try timeout; every backend failing; the body limit; the prober marking a
  backend down and up.
- **Admin:** the backend lifecycle and its error codes.

## Limitations

- HTTP/1.1 reverse proxying only: no TLS termination, WebSockets or HTTP/2 to backends, and no connection draining
  when a backend is removed.
- One balancer process. The pool lives in memory, so two balancers would each need the backend list.
- The experiment runs everything on one laptop over loopback, so absolute latencies are far lower than across a
  network. The comparison between configurations is the point, not the milliseconds.

Built with AI assistance (Claude Code). I reviewed the code; the experiment and the tests are how I verified it,
and they caught the bug above.
