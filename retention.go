package nntppool

import "time"

// retentionBiasFactor scales the dispatch weight of a provider that declares a
// finite MaxArticleAge and is in range for this article. A provider with half
// the connections of an unlimited one then absorbs a comparable share of
// in-range traffic, which is the point of declaring the limit: recent articles
// are served by the provider that can only serve recent articles, leaving the
// deep-retention provider's connections and quota for the articles that have
// nowhere else to go.
//
// It is deliberately a small integer. Dispatch weights are integers derived
// from available connections and a speed score, and a bias large enough to
// swamp those would turn "prefer" into "pin".
const retentionBiasFactor = 2

// retention is how a provider relates to one article's age.
type retention uint8

const (
	// retentionInRange: the provider serves this article's age — either it
	// declares no limit, or the article falls inside it.
	retentionInRange retention = iota
	// retentionOverAge: the article is older than the provider's declared
	// retention, and the provider did not ask for that to be binding. It is
	// tried, but only after every in-range provider.
	retentionOverAge
	// retentionExcluded: as above, but the provider set StrictMaxAge, so it is
	// not contacted for this article at all.
	retentionExcluded
)

// classify reports how g relates to an article posted at date. A zero date
// means the caller does not know when the article was posted, which is not the
// same as knowing it is old: no retention policy applies, and every provider
// is in range. Likewise MaxArticleAge == 0 means the provider declares no
// limit.
//
// now is passed in so one pass over the providers shares a single clock read,
// and so tests need no clock injection.
func (g *providerGroup) classify(date, now time.Time) retention {
	maxAge := g.p.MaxArticleAge
	if maxAge <= 0 || date.IsZero() {
		return retentionInRange
	}
	if now.Sub(date) <= maxAge {
		return retentionInRange
	}
	if g.p.StrictMaxAge {
		return retentionExcluded
	}
	return retentionOverAge
}

// retentionOrder returns the indices of gs to attempt, in order: every in-range
// provider first, then every over-age one. Strictly-excluded providers are left
// out entirely.
//
// The returned slice is nil when no provider declares a retention limit that
// bears on this article, which is the common case: callers read nil as "the
// natural order applies" and skip the reordering work altogether.
//
// If every provider is strictly excluded the strict flag is ignored and the
// natural order is returned. A misconfigured age must not be able to make an
// article unreachable — the pool's job is to answer the question, and a
// provider that says it has the article outranks a local guess that it cannot.
func retentionOrder(gs []*providerGroup, date, now time.Time) (order []int) {
	if date.IsZero() {
		return nil
	}
	var inRange, overAge []int
	relevant := false
	for i, g := range gs {
		switch g.classify(date, now) {
		case retentionInRange:
			inRange = append(inRange, i)
		case retentionOverAge:
			overAge = append(overAge, i)
			relevant = true
		case retentionExcluded:
			relevant = true
		}
	}
	if !relevant {
		return nil // nothing to reorder: every provider serves this age
	}
	if len(inRange) == 0 && len(overAge) == 0 {
		return nil // every provider strictly excluded; ignore the flag
	}
	return append(inRange, overAge...)
}

// inRangeCount reports how many leading entries of an order built by
// retentionOrder are in range, i.e. how much of it dispatch may choose a start
// index within. Over-age providers are a fallback tier, not candidates to open
// with, so weighted round-robin never starts inside them.
func inRangeCount(gs []*providerGroup, order []int, date, now time.Time) int {
	n := 0
	for _, idx := range order {
		if gs[idx].classify(date, now) != retentionInRange {
			break
		}
		n++
	}
	return n
}
