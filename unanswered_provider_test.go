package nntppool

import (
	"context"
	"errors"
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
