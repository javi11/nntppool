# Migrating from v4 to v5

v5 replaces the eleven article-request methods of v4 with four, and moves what
varied between them — lane, buffered vs. streamed, metadata callback — into
fields on a `Req` struct.

## Why

In v4 a request's shape was encoded in its method name. Two axes (three lanes ×
buffered/streamed/async) already spent ten names, and the `onMeta` variadic
occupied the only slot a future option could have used. Any third axis —
per-article retention being the one that forced this — would have multiplied the
surface again or arrived as an invisible context value.

`Req` makes those axes orthogonal fields. Adding one is now a field, not a
combinatorial explosion, and every call site reads as a description of the
request rather than a name to decode.

## Update the import path

```diff
-import "github.com/javi11/nntppool/v4"
+import "github.com/javi11/nntppool/v5"
```

v4 remains importable and frozen at its own module path. It receives no
backports.

## Method mapping

| v4 | v5 |
|----|----|
| `Body(ctx, id)` | `Fetch(ctx, Req{MessageID: id})` |
| `Body(ctx, id, onMeta)` | `Fetch(ctx, Req{MessageID: id, OnMeta: onMeta})` |
| `BodyPriority(ctx, id)` | `Fetch(ctx, Req{MessageID: id, Lane: LanePriority})` |
| `BodyBackground(ctx, id)` | `Fetch(ctx, Req{MessageID: id, Lane: LaneBackground})` |
| `BodyStream(ctx, id, w)` | `Fetch(ctx, Req{MessageID: id, Writer: w})` |
| `BodyStreamPriority(ctx, id, w)` | `Fetch(ctx, Req{MessageID: id, Writer: w, Lane: LanePriority})` |
| `BodyAsync(ctx, id, w)` | `FetchAsync(ctx, Req{MessageID: id, Writer: w})` |
| `Stat(ctx, id)` | `Exists(ctx, Req{MessageID: id})` |
| `StatPriority(ctx, id)` | `Exists(ctx, Req{MessageID: id, Lane: LanePriority})` |
| `StatBackground(ctx, id)` | `Exists(ctx, Req{MessageID: id, Lane: LaneBackground})` |
| `StatAsync(ctx, id)` | `ExistsAsync(ctx, Req{MessageID: id})` |
| `StatMany(ctx, ids, opts)` | `ExistsMany(ctx, ids, opts)` |
| `Send(ctx, payload, w)` | `Send(ctx, SendReq{Payload: payload, Writer: w})` |
| `SendPriority(ctx, payload, w)` | `Send(ctx, SendReq{Payload: payload, Writer: w, Lane: LanePriority})` |
| `SendBackground(ctx, payload, w)` | `Send(ctx, SendReq{Payload: payload, Writer: w, Lane: LaneBackground})` |
| `Head(ctx, id)` | unchanged |
| `PostYenc`, `PostYencTo` | unchanged |

## Renamed types

| v4 | v5 |
|----|----|
| `StatManyOptions` | `ManyOptions` |
| `StatManyResult` | `ExistsResult` |
| `StatManyOptions.Priority: true` | `ManyOptions.Lane: LanePriority` |
| `StatManyOptions.Background: true` | `ManyOptions.Lane: LaneBackground` |

`Lane` is now exported, with `LaneNormal` (the zero value), `LanePriority`, and
`LaneBackground`. `ManyOptions` keeps `Concurrency`, `Provider`, and `Skip`
unchanged.

## Behavior changes

Three, all at the edges:

**A nil writer now means buffered, not invalid.** In v4, `BodyStream` and
`BodyStreamPriority` rejected a nil writer, because the buffered mode lived in a
different method. In v5 there is one method, so `Req.Writer == nil` selects
buffered delivery and returns the payload in `ArticleBody.Bytes`. Code that
relied on the nil-writer error must check for itself that it passed a writer.

**An empty message-ID is rejected.** `Fetch`, `FetchAsync`, `Exists`, and
`ExistsAsync` return `ErrNoMessageID` without dispatching. v4 sent `BODY <>` and
paid a round-trip to be told what the caller already knew.

**`FetchAsync` can buffer.** v4's `BodyAsync` took the writer positionally and
had no buffered form, so callers passed `io.Discard` and lost the payload.
Omitting `Req.Writer` now buffers into `Body.Bytes`.

Everything else — failover order, the 430 STAT probe, attempt-window escalation,
lane preference, quota accounting, `Provider` configuration, `Stats`, `AddProvider`
/`RemoveProvider` — is unchanged.

## Mechanical migration

Most call sites convert with a search and replace. A regex that covers the
common two- and three-argument forms:

```
s/\.Body\((\w+), (".*?"|\w+)\)/.Fetch($1, nntppool.Req{MessageID: $2})/
s/\.Stat\((\w+), (".*?"|\w+)\)/.Exists($1, nntppool.Req{MessageID: $2})/
```

After converting, `go build ./...` finds the rest: every removed method is a
compile error naming its own call site, and the table above gives the
replacement.
