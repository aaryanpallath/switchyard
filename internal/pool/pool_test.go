package pool

import (
	"errors"
	"testing"
	"time"
)

func newPool(t *testing.T, cfg Config, urls ...string) (*Pool, []*Backend) {
	t.Helper()
	p := New(cfg)
	var bs []*Backend
	for _, u := range urls {
		b, err := p.Add(u)
		if err != nil {
			t.Fatal(err)
		}
		bs = append(bs, b)
	}
	return p, bs
}

func TestAddValidatesAndRejectsDuplicates(t *testing.T) {
	p := New(DefaultConfig())
	for _, bad := range []string{"", "10.0.0.1:80", "ftp://x", "http://"} {
		if _, err := p.Add(bad); err == nil {
			t.Errorf("Add(%q) should fail", bad)
		}
	}
	if _, err := p.Add("http://a:1"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Add("http://a:1"); !errors.Is(err, ErrDuplicate) {
		t.Fatalf("duplicate: got %v", err)
	}
	if err := p.Remove("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("remove unknown: got %v", err)
	}
}

func TestRoundRobinSkipsUnavailableAndExcluded(t *testing.T) {
	p, bs := newPool(t, DefaultConfig(), "http://a:1", "http://b:1", "http://c:1")
	for range p.cfg.FailThreshold {
		p.ProbeResult(bs[1], false)
	}
	seen := map[string]int{}
	for range 10 {
		b, err := p.Pick(nil)
		if err != nil {
			t.Fatal(err)
		}
		seen[b.ID]++
		p.Done(b, true)
	}
	if seen[bs[1].ID] != 0 || seen[bs[0].ID] != 5 || seen[bs[2].ID] != 5 {
		t.Fatalf("down backend must get nothing and the rest share evenly: %v", seen)
	}
	b, _ := p.Pick(map[string]bool{bs[0].ID: true})
	if b != bs[2] {
		t.Fatalf("excluded backend picked: %s", b.ID)
	}
	p.Done(b, true)
	if _, err := p.Pick(map[string]bool{bs[0].ID: true, bs[2].ID: true}); !errors.Is(err, ErrNoBackend) {
		t.Fatalf("want ErrNoBackend, got %v", err)
	}
}

func TestHealthThresholdsNeedConsecutiveResults(t *testing.T) {
	p, bs := newPool(t, DefaultConfig(), "http://a:1")
	b := bs[0]
	p.ProbeResult(b, false)
	p.ProbeResult(b, true) // the streak resets
	p.ProbeResult(b, false)
	if !p.Status()[0].Up {
		t.Fatal("one failure at a time must not mark a backend down")
	}
	p.ProbeResult(b, false)
	if p.Status()[0].Up {
		t.Fatal("two failures in a row must mark it down")
	}
	p.ProbeResult(b, true)
	if p.Status()[0].Up {
		t.Fatal("one pass must not bring it back")
	}
	p.ProbeResult(b, true)
	if !p.Status()[0].Up {
		t.Fatal("two passes in a row must bring it back")
	}
}

func TestPassiveEjectionAndExpiry(t *testing.T) {
	now := time.Unix(1000, 0)
	p, bs := newPool(t, DefaultConfig(), "http://a:1", "http://b:1")
	p.SetClock(func() time.Time { return now })
	fail := func(b *Backend, ok bool) {
		b.active.Add(1) // as Pick would
		p.Done(b, ok)
	}
	fail(bs[0], false)
	fail(bs[0], false)
	fail(bs[0], true) // a success resets the streak
	fail(bs[0], false)
	fail(bs[0], false)
	if p.Status()[0].Ejected {
		t.Fatal("ejected before 3 failures in a row")
	}
	fail(bs[0], false)
	if !p.Status()[0].Ejected {
		t.Fatal("not ejected after 3 failures in a row")
	}
	for range 4 {
		b, _ := p.Pick(nil)
		if b == bs[0] {
			t.Fatal("ejected backend picked")
		}
		p.Done(b, true)
	}
	now = now.Add(p.cfg.EjectFor)
	if p.Status()[0].Ejected {
		t.Fatal("ejection must expire")
	}
	if s := p.Status()[0]; s.Served != 6 || s.Failed != 5 {
		t.Fatalf("counts: %+v", s)
	}
}

func TestLeastConnectionsPrefersIdleBackends(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Strategy = LeastConnections
	p, bs := newPool(t, cfg, "http://a:1", "http://b:1")
	busy, _ := p.Pick(nil) // held open, as a slow request would be
	for range 5 {
		b, _ := p.Pick(nil)
		if b == busy {
			t.Fatal("picked the backend with a request in flight")
		}
		p.Done(b, true)
	}
	p.Done(busy, true)
	if bs[0].active.Load() != 0 || bs[1].active.Load() != 0 {
		t.Fatal("in-flight counts must return to zero")
	}
}
