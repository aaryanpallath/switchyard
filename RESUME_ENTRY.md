# switchyard — Fault-Tolerant HTTP Load Balancer in Go
Tech: Go, REST APIs, HTTP, distributed systems, Linux, GitHub Actions
- Built an HTTP load balancer in Go with a REST admin API, active health checks, passive ejection of failing backends, idempotent-only retries with per-try timeouts, and round-robin or least-connections routing.
- Designed a failure-injection experiment (dead, flaky and slow backends under 32 concurrent clients): client errors fell from 29% to 0% and p99 latency from 1.5 s to 4 ms as each feature was added.
- Showed health checks alone missed a backend failing half its requests (16.6% errors) and fixed it with passive ejection; the experiment also exposed a crash on refused connections, now covered by a race-tested regression suite.
