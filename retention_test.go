package nntppool

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

func groupFor(p Provider) *providerGroup {
	return &providerGroup{p: p, name: p.Host}
}

func TestClassify(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	tests := []struct {
		name string
		p    Provider
		date time.Time
		want retention
	}{
		{"no limit declared", Provider{}, now.Add(-4000 * day), retentionInRange},
		{"in range", Provider{MaxArticleAge: 100 * day}, now.Add(-30 * day), retentionInRange},
		{"exactly at the limit", Provider{MaxArticleAge: 100 * day}, now.Add(-100 * day), retentionInRange},
		{"just over the limit", Provider{MaxArticleAge: 100 * day}, now.Add(-101 * day), retentionOverAge},
		{"over, strict", Provider{MaxArticleAge: 100 * day, StrictMaxAge: true}, now.Add(-101 * day), retentionExcluded},
		{"in range, strict", Provider{MaxArticleAge: 100 * day, StrictMaxAge: true}, now.Add(-30 * day), retentionInRange},
		// An unknown date is not a claim that the article is old. Retention
		// cannot apply, so every provider stays in range — the property that
		// makes the feature safe for metadata written before post dates existed.
		{"unknown date, strict limit", Provider{MaxArticleAge: 100 * day, StrictMaxAge: true}, time.Time{}, retentionInRange},
		// A date in the future (clock skew, a bad header) is "newer than
		// anything", so every provider reaches it.
		{"future date", Provider{MaxArticleAge: 100 * day}, now.Add(10 * day), retentionInRange},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := groupFor(tc.p).classify(tc.date, now); got != tc.want {
				t.Fatalf("classify() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRetentionOrder(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	unlimited := groupFor(Provider{Host: "unlimited"})
	deep := groupFor(Provider{Host: "deep", MaxArticleAge: 1500 * day})
	shallow := groupFor(Provider{Host: "shallow", MaxArticleAge: 100 * day})
	strict := groupFor(Provider{Host: "strict", MaxArticleAge: 100 * day, StrictMaxAge: true})

	names := func(gs []*providerGroup, order []int) []string {
		if order == nil {
			return nil
		}
		out := make([]string, len(order))
		for i, idx := range order {
			out[i] = gs[idx].name
		}
		return out
	}

	tests := []struct {
		name string
		gs   []*providerGroup
		date time.Time
		want []string // nil means "natural order applies"
	}{
		{
			name: "unknown date leaves the order alone",
			gs:   []*providerGroup{shallow, unlimited},
			date: time.Time{},
			want: nil,
		},
		{
			name: "article in range everywhere leaves the order alone",
			gs:   []*providerGroup{shallow, unlimited, strict},
			date: now.Add(-30 * day),
			want: nil,
		},
		{
			name: "over-age provider is demoted behind the in-range ones",
			gs:   []*providerGroup{shallow, unlimited, deep},
			date: now.Add(-400 * day),
			want: []string{"unlimited", "deep", "shallow"},
		},
		{
			name: "strict over-age provider is dropped",
			gs:   []*providerGroup{strict, unlimited},
			date: now.Add(-400 * day),
			want: []string{"unlimited"},
		},
		{
			name: "demoted providers keep their declared order among themselves",
			gs:   []*providerGroup{shallow, deep, groupFor(Provider{Host: "shallow2", MaxArticleAge: 50 * day}), unlimited},
			date: now.Add(-400 * day),
			want: []string{"deep", "unlimited", "shallow", "shallow2"},
		},
		{
			// The escape hatch: honouring every strict flag would leave the
			// request nowhere to go, so the flags are ignored rather than
			// making the article unreachable.
			name: "all strictly excluded falls back to the natural order",
			gs:   []*providerGroup{strict, groupFor(Provider{Host: "strict2", MaxArticleAge: 10 * day, StrictMaxAge: true})},
			date: now.Add(-400 * day),
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := names(tc.gs, retentionOrder(tc.gs, tc.date, now))
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("retentionOrder() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestInRangeCountStopsAtFirstDemotion(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour
	gs := []*providerGroup{
		groupFor(Provider{Host: "shallow", MaxArticleAge: 100 * day}),
		groupFor(Provider{Host: "unlimited"}),
		groupFor(Provider{Host: "deep", MaxArticleAge: 1500 * day}),
	}
	date := now.Add(-400 * day)
	order := retentionOrder(gs, date, now)
	if got, want := inRangeCount(gs, order, date, now), 2; got != want {
		t.Fatalf("inRangeCount() = %d, want %d (unlimited + deep)", got, want)
	}
}

// retentionBiasFactor is applied to an in-range provider that declares a
// limit, so it absorbs a share of recent traffic out of proportion to its
// connection count — leaving the unlimited provider's connections for articles
// only it can serve.
func TestDispatchWeightsBiasesInRangeLimitedProvider(t *testing.T) {
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	day := 24 * time.Hour

	newG := func(p Provider, avail int32) *providerGroup {
		g := groupFor(p)
		g.gate = newConnGate(int(avail), 0)
		g.gate.available.Store(avail)
		return g
	}

	unlimited := newG(Provider{Host: "unlimited"}, 20)
	shallow := newG(Provider{Host: "shallow", MaxArticleAge: 100 * day}, 10)
	gs := []*providerGroup{unlimited, shallow}

	// Recent article: the shallow provider's 10 connections are biased to 20,
	// matching the unlimited provider's 20.
	_, total := dispatchWeights(gs, false, now.Add(-30*day), now)
	if want := 20 + 10*retentionBiasFactor; total != want {
		t.Fatalf("in-range total weight = %d, want %d", total, want)
	}

	// Old article: the shallow provider is over-age, so no bias applies. (It
	// is demoted out of the choosable head by retentionOrder; pricing it here
	// only has to not reward it.)
	_, total = dispatchWeights(gs, false, now.Add(-400*day), now)
	if want := 20 + 10; total != want {
		t.Fatalf("over-age total weight = %d, want %d", total, want)
	}

	// No date: prices exactly as it did before retention existed.
	_, total = dispatchWeights(gs, false, time.Time{}, time.Time{})
	if want := 20 + 10; total != want {
		t.Fatalf("dateless total weight = %d, want %d", total, want)
	}
}

// retentionProbeFactory answers every STAT with reply and records that it was asked.
func retentionProbeFactory(t testing.TB, mu *sync.Mutex, asked *[]string, name, reply string) ConnFactory {
	t.Helper()
	return func(ctx context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			defer func() { _ = server.Close() }()
			_, _ = server.Write([]byte("200 server ready\r\n"))
			buf := make([]byte, 4096)
			for {
				n, err := server.Read(buf)
				if err != nil {
					return
				}
				cmd := strings.TrimRight(string(buf[:n]), "\r\n")
				if strings.HasPrefix(cmd, "DATE") {
					_, _ = server.Write([]byte("111 20240101000000\r\n"))
					continue
				}
				mu.Lock()
				*asked = append(*asked, name)
				mu.Unlock()
				_, _ = fmt.Fprintf(server, "%s\r\n", reply)
			}
		}()
		return client, nil
	}
}

// End-to-end: a strictly-excluded provider is never contacted for an article
// older than its retention, and the article still resolves from the provider
// that does reach it.
func TestExistsSkipsStrictOverAgeProvider(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	day := 24 * time.Hour

	c, err := NewClient(context.Background(), []Provider{
		{
			Host:          "shallow:119",
			Factory:       retentionProbeFactory(t, &mu, &asked, "shallow", "430 no such article"),
			Connections:   1,
			MaxArticleAge: 100 * day,
			StrictMaxAge:  true,
		},
		{
			Host:        "deep:119",
			Factory:     retentionProbeFactory(t, &mu, &asked, "deep", "223 1 <old@h> exists"),
			Connections: 1,
		},
	}, WithStatProbe(false), WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := c.Exists(ctx, Req{MessageID: "old@h", ArticleDate: time.Now().Add(-400 * day)})
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}
	if res.Provider != "deep:119" {
		t.Fatalf("answered by %q, want deep:119", res.Provider)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, who := range asked {
		if who == "shallow" {
			t.Fatalf("strictly-excluded provider was contacted: %v", asked)
		}
	}
}

// A demoted (non-strict) provider is still reachable: when the in-range
// providers all miss, the over-age tier is tried rather than the request
// failing. This is the property that keeps a wrong date from making an article
// unreachable.
func TestExistsFallsBackToDemotedProvider(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	day := 24 * time.Hour

	c, err := NewClient(context.Background(), []Provider{
		{
			Host:        "deep:119",
			Factory:     retentionProbeFactory(t, &mu, &asked, "deep", "430 no such article"),
			Connections: 1,
		},
		{
			Host:          "shallow:119",
			Factory:       retentionProbeFactory(t, &mu, &asked, "shallow", "223 1 <old@h> exists"),
			Connections:   1,
			MaxArticleAge: 100 * day,
		},
	}, WithStatProbe(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := c.Exists(ctx, Req{MessageID: "old@h", ArticleDate: time.Now().Add(-400 * day)})
	if err != nil {
		t.Fatalf("Exists() error = %v, want the demoted provider to answer", err)
	}
	if res.Provider != "shallow:119" {
		t.Fatalf("answered by %q, want shallow:119", res.Provider)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(asked) < 2 || asked[0] != "deep" {
		t.Fatalf("attempt order = %v, want the in-range provider first", asked)
	}
}

// With every provider strictly excluded the flags are ignored: the request is
// attempted rather than failed.
func TestExistsIgnoresStrictWhenItWouldExcludeEveryone(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	day := 24 * time.Hour

	c, err := NewClient(context.Background(), []Provider{{
		Host:          "shallow:119",
		Factory:       retentionProbeFactory(t, &mu, &asked, "shallow", "223 1 <old@h> exists"),
		Connections:   1,
		MaxArticleAge: 100 * day,
		StrictMaxAge:  true,
	}}, WithStatProbe(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := c.Exists(ctx, Req{MessageID: "old@h", ArticleDate: time.Now().Add(-400 * day)}); err != nil {
		t.Fatalf("Exists() error = %v, want the strict flag ignored", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(asked) == 0 {
		t.Fatal("no provider was contacted; the request was dropped rather than attempted")
	}
}

// A sweep carries one date for the whole release, and it must reach dispatch:
// otherwise a health census hammers the short-retention provider with ancient
// ids.
func TestExistsManyHonoursArticleDate(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	day := 24 * time.Hour

	c, err := NewClient(context.Background(), []Provider{
		{
			Host:          "shallow:119",
			Factory:       retentionProbeFactory(t, &mu, &asked, "shallow", "430 no such article"),
			Connections:   1,
			MaxArticleAge: 100 * day,
			StrictMaxAge:  true,
		},
		{
			Host:        "deep:119",
			Factory:     retentionProbeFactory(t, &mu, &asked, "deep", "223 1 <x@h> exists"),
			Connections: 1,
		},
	}, WithStatProbe(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ids := []string{"a@h", "b@h", "c@h"}
	got := collectStat(c.ExistsMany(context.Background(), ids, ManyOptions{
		ArticleDate: time.Now().Add(-400 * day),
	}))
	for _, id := range ids {
		if r := got[id]; r.Err != nil {
			t.Fatalf("%s: err = %v", id, r.Err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for _, who := range asked {
		if who == "shallow" {
			t.Fatalf("sweep contacted the strictly-excluded provider: %v", asked)
		}
	}
}

// A retention limit must not turn a genuine miss into a different error: the
// article is absent everywhere, and that is what the caller hears.
func TestExistsMissStaysNotFoundUnderRetention(t *testing.T) {
	var mu sync.Mutex
	var asked []string
	day := 24 * time.Hour

	c, err := NewClient(context.Background(), []Provider{
		{
			Host:          "shallow:119",
			Factory:       retentionProbeFactory(t, &mu, &asked, "shallow", "430 no such article"),
			Connections:   1,
			MaxArticleAge: 100 * day,
		},
		{
			Host:        "deep:119",
			Factory:     retentionProbeFactory(t, &mu, &asked, "deep", "430 no such article"),
			Connections: 1,
		},
	}, WithStatProbe(false))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = c.Exists(ctx, Req{MessageID: "gone@h", ArticleDate: time.Now().Add(-400 * day)})
	if !errors.Is(err, ErrArticleNotFound) {
		t.Fatalf("Exists() error = %v, want ErrArticleNotFound", err)
	}
}
