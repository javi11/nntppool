package nntppool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var missingEverywhere = map[string]string{"BODY": "430 no such article", "STAT": "430 no such article"}

// One provider lacks the article; the other holds it but is slower than the
// attempt window to answer the failover STAT probe. The article exists, so it
// must not be reported missing.
func TestMissNotReportedWhileFailoverProbeIsSlow(t *testing.T) {
	var mu sync.Mutex
	var p1Cmds []string
	var p2Body atomic.Int32
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: articleProviderFactory(1500*time.Millisecond, &p2Body), Connections: 1, AttemptTimeout: 300 * time.Millisecond},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 222 {
		t.Fatalf("status = %d (err %v), BODY calls on provider 2 = %d; want 222 for an article provider 2 holds",
			resp.StatusCode, resp.Err, p2Body.Load())
	}
}

// Three mains: the first lacks the article, the other two hold it but both
// answer the failover STAT probe after the attempt window.
func TestMissNotReportedWhenEveryFailoverProbeIsSlow(t *testing.T) {
	var mu sync.Mutex
	var p1Cmds []string
	var p2Body, p3Body atomic.Int32
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: articleProviderFactory(1500*time.Millisecond, &p2Body), Connections: 1, AttemptTimeout: 300 * time.Millisecond},
		{Factory: articleProviderFactory(1500*time.Millisecond, &p3Body), Connections: 1, AttemptTimeout: 300 * time.Millisecond},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 222 {
		t.Fatalf("status = %d (err %v), BODY calls p2=%d p3=%d; want 222 for an article two providers hold",
			resp.StatusCode, resp.Err, p2Body.Load(), p3Body.Load())
	}
}

// Both mains lack the article; the backup holds it but is slow to answer the
// STAT probe.
func TestMissNotReportedWhileBackupProbeIsSlow(t *testing.T) {
	var mu sync.Mutex
	var p1Cmds, p2Cmds []string
	var backupBody atomic.Int32
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: makeStatProbeFactory(t, &mu, &p2Cmds, missingEverywhere), Connections: 1},
		{Factory: articleProviderFactory(1500*time.Millisecond, &backupBody), Connections: 1, AttemptTimeout: 300 * time.Millisecond, Backup: true},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 222 {
		t.Fatalf("status = %d (err %v), backup BODY calls = %d; want 222 for an article the backup holds",
			resp.StatusCode, resp.Err, backupBody.Load())
	}
}

// One main lacks the article, the other holds it but its BODY starts after
// the attempt window, and the backup lacks it too.
func TestMissNotReportedWhileOnlyHolderIsSlowToStart(t *testing.T) {
	var mu sync.Mutex
	var p1Cmds, backupCmds []string
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: slowBodyFactory(1500*time.Millisecond, agedArticle), Connections: 1, AttemptTimeout: 300 * time.Millisecond, SkipPing: true},
		{Factory: makeStatProbeFactory(t, &mu, &backupCmds, missingEverywhere), Connections: 1, Backup: true},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 222 {
		t.Fatalf("status = %d (err %v); want 222 for an article provider 2 holds", resp.StatusCode, resp.Err)
	}
}

// A provider that never answers leaves the article unconfirmed: the request
// must not report it missing even though the other provider said 430, and
// the confirmation is bounded by one widest attempt window.
func TestSilentProviderGivesUnconfirmedNotMissing(t *testing.T) {
	var mu sync.Mutex
	var p1Cmds []string
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: slowBodyFactory(time.Minute, agedArticle), Connections: 1, SkipPing: true, AttemptTimeout: 300 * time.Millisecond},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	elapsed := time.Since(start)
	if resp.StatusCode == 430 || errors.Is(resp.Err, ErrArticleNotFound) {
		t.Fatalf("got a miss (status %d, err %v) while a provider never answered", resp.StatusCode, resp.Err)
	}
	if !errors.Is(resp.Err, ErrArticleUnconfirmed) {
		t.Fatalf("err = %v, want ErrArticleUnconfirmed", resp.Err)
	}
	if elapsed > maxAttemptTimeout+3*time.Second {
		t.Fatalf("took %s, want the confirmation bounded by one widest window (%s)", elapsed, maxAttemptTimeout)
	}
}

// Every provider answering 430 is still a plain, prompt miss.
func TestEveryProviderMissingIsStillAPromptMiss(t *testing.T) {
	var mu sync.Mutex
	var mainCmds, backupCmds []string
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &mainCmds, missingEverywhere), Connections: 1},
		{Factory: makeStatProbeFactory(t, &mu, &backupCmds, missingEverywhere), Connections: 1, Backup: true},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	start := time.Now()
	resp := <-c.Send(context.Background(), SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 430 {
		t.Fatalf("status = %d (err %v), want 430", resp.StatusCode, resp.Err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("a definitive miss took %s, want under 1s", elapsed)
	}
}

// shortConfirmWindow shrinks confirmWindow for one test.
func shortConfirmWindow(t *testing.T, d time.Duration) {
	t.Helper()
	restore := confirmWindow
	confirmWindow = d
	t.Cleanup(func() { confirmWindow = restore })
}

// A prompt 430 next to a provider that stays silent skips the main escalation
// pass, so the confirmation is the only widened attempt the silent provider
// gets. It must count toward the escalation breaker: once the breaker opens,
// requests stop paying the wide window for that provider.
func TestConfirmationTripsEscalationBreaker(t *testing.T) {
	shortConfirmWindow(t, 400*time.Millisecond)
	var mu sync.Mutex
	var p1Cmds []string
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: hungFactory, Connections: 1, SkipPing: true, AttemptTimeout: 30 * time.Millisecond},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	silent := (*c.mainGroups.Load())[1]

	send := func() (Response, time.Duration) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		start := time.Now()
		resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
		return resp, time.Since(start)
	}
	for i := range escalationBreakerThreshold {
		if resp, _ := send(); !errors.Is(resp.Err, ErrArticleUnconfirmed) {
			t.Fatalf("request %d: err = %v (status %d), want ErrArticleUnconfirmed", i, resp.Err, resp.StatusCode)
		}
	}
	if !silent.escalationSuppressed(time.Now()) {
		t.Fatalf("breaker closed after %d fruitless confirmations (fruitless = %d)",
			escalationBreakerThreshold, silent.stats.escFruitless.Load())
	}
	resp, elapsed := send()
	if !errors.Is(resp.Err, ErrArticleUnconfirmed) {
		t.Fatalf("err = %v (status %d), want ErrArticleUnconfirmed", resp.Err, resp.StatusCode)
	}
	if elapsed >= confirmWindow {
		t.Fatalf("request with the breaker open took %s, want under the %s confirmation window", elapsed, confirmWindow)
	}
}

// A confirmation that gets an answer resets the provider's breaker, as a
// definitive answer in the main pass does.
func TestConfirmationAnswerResetsEscalationBreaker(t *testing.T) {
	shortConfirmWindow(t, 2*time.Second)
	var mu sync.Mutex
	var p1Cmds []string
	c, err := NewClient(context.Background(), []Provider{
		{Factory: makeStatProbeFactory(t, &mu, &p1Cmds, missingEverywhere), Connections: 1},
		{Factory: slowBodyFactory(300*time.Millisecond, agedArticle), Connections: 1, SkipPing: true, AttemptTimeout: 100 * time.Millisecond},
	}, WithDispatchStrategy(DispatchFIFO))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()
	slow := (*c.mainGroups.Load())[1]
	slow.stats.escFruitless.Store(1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 222 {
		t.Fatalf("status = %d (err %v), want 222 from the slow provider", resp.StatusCode, resp.Err)
	}
	if n := slow.stats.escFruitless.Load(); n != 0 {
		t.Fatalf("fruitless escalations = %d after a confirmation answered, want 0", n)
	}
}

// B and C share storage. B stays silent; C is slower than its base window but
// answers 430 when confirmed. C's miss speaks for B as well, so the request is
// a 430 even though B was asked, and timed out, before C answered.
func TestLaterSharedStorageMissSettlesEarlierSilence(t *testing.T) {
	for _, probe := range []bool{false, true} {
		t.Run(fmt.Sprintf("statProbe=%v", probe), func(t *testing.T) {
			shortConfirmWindow(t, 400*time.Millisecond)
			var mu sync.Mutex
			var aCmds []string
			c, err := NewClient(context.Background(), []Provider{
				{Factory: makeStatProbeFactory(t, &mu, &aCmds, missingEverywhere), Connections: 1},
				{Factory: hungFactory, StorageGroup: "shared", Connections: 1, SkipPing: true, AttemptTimeout: 30 * time.Millisecond},
				{Factory: slowBodyFactory(150*time.Millisecond, noSuchArticle), StorageGroup: "shared", Connections: 1, SkipPing: true, AttemptTimeout: 30 * time.Millisecond},
			}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(probe))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer func() { _ = c.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
			if resp.StatusCode != 430 {
				t.Fatalf("status = %d (err %v), want 430: the silent provider's storage said 430", resp.StatusCode, resp.Err)
			}
		})
	}
}

// Five storage identities say 430 and a sixth provider, silent, shares the
// fifth. The four-entry skip cache is full by then, but the verdict must still
// see that the silent provider's storage answered: a prompt 430, with no wide
// confirmation attempt.
func TestMissVerdictRemembersEveryStorageIdentity(t *testing.T) {
	var mu sync.Mutex
	logs := make([][]string, 5)
	providers := make([]Provider, 0, 6)
	for i := range 5 {
		providers = append(providers, Provider{
			Factory:      makeStatProbeFactory(t, &mu, &logs[i], missingEverywhere),
			StorageGroup: fmt.Sprintf("storage-%d", i+1),
			Connections:  1,
		})
	}
	providers = append(providers, Provider{
		Factory: hungFactory, StorageGroup: "storage-5", Connections: 1, SkipPing: true, AttemptTimeout: 30 * time.Millisecond,
	})
	c, err := NewClient(context.Background(), providers, WithDispatchStrategy(DispatchFIFO), WithStatProbe(false))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	start := time.Now()
	resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
	if resp.StatusCode != 430 {
		t.Fatalf("status = %d (err %v), want 430: every storage identity said 430", resp.StatusCode, resp.Err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("took %s; the silent provider's storage had already answered, so no confirmation was needed", elapsed)
	}
}

// The caller's deadline expiring during a confirmation is reported as that
// deadline, not as an unconfirmed article, whichever of the context and the
// connection's read deadline notices first. Repeated because that order is a
// race.
func TestCallerDeadlineDuringConfirmationStaysADeadline(t *testing.T) {
	for _, probe := range []bool{false, true} {
		t.Run(fmt.Sprintf("statProbe=%v", probe), func(t *testing.T) {
			var mu sync.Mutex
			var aCmds []string
			c, err := NewClient(context.Background(), []Provider{
				{Factory: makeStatProbeFactory(t, &mu, &aCmds, missingEverywhere), Connections: 1},
				{Factory: hungFactory, Connections: 1, SkipPing: true, AttemptTimeout: 20 * time.Millisecond},
			}, WithDispatchStrategy(DispatchFIFO), WithStatProbe(probe))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer func() { _ = c.Close() }()

			for i := range 10 {
				ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
				resp := <-c.Send(ctx, SendReq{Payload: []byte("BODY <test@host>\r\n")})
				cancel()
				if !errors.Is(resp.Err, context.DeadlineExceeded) || errors.Is(resp.Err, ErrArticleUnconfirmed) {
					t.Fatalf("request %d: err = %v (status %d), want the caller's context.DeadlineExceeded", i, resp.Err, resp.StatusCode)
				}
			}
		})
	}
}
