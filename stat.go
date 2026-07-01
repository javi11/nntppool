package nntppool

import (
	"context"
	"fmt"
	"sync"
)

// defaultStatConcurrency bounds in-flight STATs when StatManyOptions.Concurrency
// is unset. STAT is a single-line request with a single-line reply and no body,
// so it is purely round-trip-latency bound; a high default lets the pool amortise
// RTT by keeping many checks outstanding across all connections at once.
const defaultStatConcurrency = 64

// StatManyResult is the per-message outcome streamed by StatMany and StatAsync.
// A genuine miss (article not found, NNTP 430/423) is reported as
// Err == ErrArticleNotFound with a nil Result — it is a normal outcome of an
// existence sweep, not a fatal error.
type StatManyResult struct {
	MessageID string
	Result    *StatResult // non-nil on 2xx
	Err       error
}

// StatManyOptions tunes a StatMany sweep.
type StatManyOptions struct {
	// Concurrency bounds the number of STATs outstanding across the whole pool
	// at once. <= 0 uses defaultStatConcurrency.
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
func (c *Client) StatMany(ctx context.Context, messageIDs []string, opts StatManyOptions) <-chan StatManyResult {
	if ctx == nil {
		ctx = context.Background()
	}
	conc := opts.Concurrency
	if conc <= 0 {
		conc = defaultStatConcurrency
	}
	if conc > len(messageIDs) && len(messageIDs) > 0 {
		conc = len(messageIDs)
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

		sem := make(chan struct{}, conc)
		var wg sync.WaitGroup

	dispatch:
		for _, id := range messageIDs {
			select {
			case <-ctx.Done():
				break dispatch
			case sem <- struct{}{}:
			}

			wg.Add(1)
			go func(id string) {
				defer wg.Done()
				defer func() { <-sem }()

				res := c.statOne(ctx, id, target, targetErr, opts.Priority)
				select {
				case out <- res:
				case <-ctx.Done():
				}
			}(id)
		}

		wg.Wait()
	}()

	return out
}

// statOne performs a single STAT and maps it to a StatManyResult. When target is
// set the check is confined to that provider group; otherwise it uses the
// pool-wide failover path (Send/SendPriority).
func (c *Client) statOne(ctx context.Context, messageID string, target *providerGroup, targetErr error, priority bool) StatManyResult {
	if targetErr != nil {
		return StatManyResult{MessageID: messageID, Err: targetErr}
	}

	payload := statPayload(messageID)

	var resp Response
	switch {
	case target != nil:
		resp = c.statViaGroup(ctx, target, payload, priority)
	case priority:
		resp = <-c.SendPriority(ctx, payload, nil)
	default:
		resp = <-c.Send(ctx, payload, nil)
	}

	result, err := parseStat(messageID, resp)
	return StatManyResult{MessageID: messageID, Result: result, Err: err}
}

// statViaGroup issues a STAT against a single provider group, reusing the same
// resilient single-group send (with fresh-connection retry on connection death)
// that the failover path uses per provider. No cross-provider failover.
func (c *Client) statViaGroup(ctx context.Context, g *providerGroup, payload []byte, priority bool) Response {
	resp, ok, cancelled := c.tryGroupResilient(ctx, g, payload, nil, nil, priority)
	switch {
	case cancelled:
		err := ctx.Err()
		if err == nil {
			err = c.ctx.Err()
		}
		return Response{Err: err}
	case !ok:
		return Response{Err: ErrConnectionDied}
	default:
		return resp
	}
}
