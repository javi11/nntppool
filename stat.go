package nntppool

import (
	"context"
	"fmt"
	"sync"
)

// minStatConcurrency floors the derived in-flight STAT bound when
// StatManyOptions.Concurrency is unset. STAT is a single-line request with a
// single-line reply and no body, so it is purely round-trip-latency bound; a
// high floor lets even a small pool amortise RTT by keeping many checks
// outstanding at once.
const minStatConcurrency = 64

// maxStatConcurrency caps the derived bound: past a few thousand outstanding
// STATs the connections' own pipelines are saturated and more dispatch
// goroutines only cost memory.
const maxStatConcurrency = 4096

// statCapacity is the pool's aggregate STAT pipeline depth — for every
// provider, connections × the per-connection bodyless-STAT inflight cap —
// clamped to [minStatConcurrency, maxStatConcurrency]. It is what a sweep must
// keep outstanding to fill every connection's pipeline: a flat dispatch bound
// below it leaves wire idle exactly on the STAT-heavy workloads StatInflight
// exists for.
func (c *Client) statCapacity() int {
	total := 0
	for _, groups := range [...]*[]*providerGroup{c.mainGroups.Load(), c.backupGroups.Load()} {
		for _, g := range *groups {
			// Mirror startProviderGroup's effective depth: Inflight floors at
			// 1, and StatInflight below it means "same as Inflight".
			inflight := max(g.p.Inflight, 1)
			statInflight := max(g.p.StatInflight, inflight)
			total += max(g.maxConns, 1) * statInflight
		}
	}
	return min(max(total, minStatConcurrency), maxStatConcurrency)
}

// StatManyResult is the per-message outcome streamed by StatMany, StatStream
// and StatAsync.
// A genuine miss (article not found, NNTP 430/423) is reported as
// Err == ErrArticleNotFound with a nil Result — it is a normal outcome of an
// existence sweep, not a fatal error.
type StatManyResult struct {
	MessageID string
	Result    *StatResult // non-nil on 2xx
	Err       error
}

// StatManyOptions tunes a StatMany or StatStream sweep.
type StatManyOptions struct {
	// Concurrency bounds the number of STATs outstanding across the whole pool
	// at once. <= 0 derives the bound from the pool's aggregate STAT pipeline
	// capacity (connections × StatInflight per provider), so every
	// connection's pipeline can fill.
	Concurrency int

	// Priority routes each STAT through the priority channel so idle connections
	// pick it up ahead of normal (e.g. BODY) traffic.
	Priority bool

	// Provider, when set, restricts every STAT to the named provider group
	// (per-provider availability audit — retention differs per provider). The
	// name matches Client provider names ("host:port" or "host:port+username").
	// When empty, STATs dispatch across the whole pool with the same
	// cross-provider/backup failover semantics as Stat ("exists anywhere").
	Provider string
}

// StatMany checks the existence of many articles concurrently, streaming a
// StatManyResult per message-id as each check completes (results arrive out of
// order). The returned channel is closed once every dispatched check has
// reported. If ctx is cancelled mid-sweep, dispatch stops, in-flight checks are
// cancelled, and the channel is closed; message-ids not yet dispatched produce
// no result, so callers should check ctx.Err() after draining.
//
// Every id is committed up front. A caller that decides mid-sweep to abandon
// work — an availability sweep that has already condemned a file, say — wants
// StatStream instead, which asks for each id only when it is about to run.
func (c *Client) StatMany(ctx context.Context, messageIDs []string, opts StatManyOptions) <-chan StatManyResult {
	if ctx == nil {
		ctx = context.Background()
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = c.statCapacity()
	}
	// The length is known here, so cap the pool at it: a sweep of 3 ids has no
	// use for statCapacity() workers.
	if len(messageIDs) > 0 && opts.Concurrency > len(messageIDs) {
		opts.Concurrency = len(messageIDs)
	}

	ids := make(chan string)
	go func() {
		defer close(ids)
		for _, id := range messageIDs {
			select {
			case ids <- id:
			case <-ctx.Done():
				return
			}
		}
	}()

	return c.StatStream(ctx, ids, opts)
}

// StatStream is StatMany fed by a channel rather than a slice, so the caller
// decides whether an id is still worth checking at the moment it would be
// dispatched rather than when the sweep is planned. A sweep that abandons work
// as verdicts arrive — a file already past its acceptable-missing threshold, a
// release already known dead — then stops paying for the ids it no longer
// needs, at single-id granularity and independently of Concurrency.
//
// Backpressure runs the other way too: receives from ids only happen as
// workers free up, so a caller blocked sending an id knows Concurrency checks
// are already outstanding.
//
// One StatManyResult is produced per id received. The result channel closes
// once ids is closed and every check has reported, or once ctx is cancelled —
// ids received before cancellation may go unreported, so check ctx.Err() after
// draining. The caller owns ids and must close it; a caller still blocked
// sending on it when ctx is cancelled is released by the same cancellation.
func (c *Client) StatStream(ctx context.Context, ids <-chan string, opts StatManyOptions) <-chan StatManyResult {
	if ctx == nil {
		ctx = context.Background()
	}
	conc := opts.Concurrency
	if conc <= 0 {
		conc = c.statCapacity()
	}

	out := make(chan StatManyResult, conc)

	// Resolve the target group once (outside the goroutine) so an unknown
	// provider name fails every id with a clear error rather than silently
	// dispatching pool-wide.
	var target *providerGroup
	var targetErr error
	if opts.Provider != "" {
		if target = c.findGroup(opts.Provider); target == nil {
			targetErr = fmt.Errorf("nntp: provider %q not found", opts.Provider)
		}
	}

	go func() {
		defer close(out)

		work := make(chan string)
		var wg sync.WaitGroup
		workers := 0

	dispatch:
		for {
			var id string
			var ok bool
			select {
			case <-ctx.Done():
				break dispatch
			case id, ok = <-ids:
				if !ok {
					break dispatch
				}
			}

			// Grow the pool on demand up to conc. A stream has no length to
			// size from, and conc is the pool's whole STAT capacity by
			// default, so spawning eagerly would cost thousands of goroutines
			// to check a handful of ids.
			if workers < conc {
				workers++
				wg.Add(1)
				go func() {
					defer wg.Done()
					for id := range work {
						res := c.statOne(ctx, id, target, targetErr, opts.Priority)
						select {
						case out <- res:
						case <-ctx.Done():
							return
						}
					}
				}()
			}

			select {
			case <-ctx.Done():
				break dispatch
			case work <- id:
			}
		}

		close(work)
		wg.Wait()
	}()

	return out
}

// statOne performs a single STAT and maps it to a StatManyResult. When target is
// set the check is confined to that provider group; otherwise it uses the
// pool-wide failover path.
func (c *Client) statOne(ctx context.Context, messageID string, target *providerGroup, targetErr error, priority bool) StatManyResult {
	if targetErr != nil {
		return StatManyResult{MessageID: messageID, Err: targetErr}
	}

	payload := statPayload(messageID)

	var resp Response
	if target != nil {
		resp = c.statViaGroup(ctx, target, payload, priority)
	} else {
		resp = c.sendSync(ctx, payload, priority)
	}

	result, err := parseStat(messageID, resp)
	return StatManyResult{MessageID: messageID, Result: result, Err: err}
}

// statViaGroup issues a STAT against a single provider group, reusing the same
// resilient single-group send (with fresh-connection retry on connection death)
// that the failover path uses per provider. No cross-provider failover.
func (c *Client) statViaGroup(ctx context.Context, g *providerGroup, payload []byte, priority bool) Response {
	resp, ok, cancelled := c.tryGroupResilient(ctx, g, payload, nil, nil, priority, 0)
	switch {
	case cancelled:
		err := ctx.Err()
		if err == nil {
			err = c.ctx.Err()
		}
		return Response{Err: err}
	case !ok:
		if resp.Err != nil {
			return resp // expired attempts keep their typed reason
		}
		return Response{Err: ErrConnectionDied}
	default:
		return resp
	}
}
