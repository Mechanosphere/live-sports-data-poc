# DynamoDB vs Redis for live sports ingest

This note records why this service persists normalized events in **DynamoDB**,
and why **Redis** belongs on the serving path (JSON for the CDN) rather than as
the ingest system of record. It includes the strongest arguments for Redis, then
why they do not win given expected volume and a two-cluster Redis topology.

Related implementation: single-table design and access patterns in the main
[README](README.md) (DynamoDB section), `app/models/keys.py`,
`app/services/football_repository.py`, `app/ingestion/kafka_consumer.py`.

## Recommendation

**Ingest and event history: DynamoDB.**  
**Frontend documents: a serving Redis cluster that materializes JSON for the CDN.**  
Do not ingest into Redis, including a dedicated second ingest Redis cluster.

Fan-out to clients is out of scope for this repo. The job here is to land
events quickly and idempotently so an existing path can project them:

```
Provider poll  →  Kafka (raw)  →  normalize  →  DynamoDB (event store)
                                              ↓ CDC
                                        Kafka (changes)
                                              ↓
                              Serving Redis (current match JSON)
                                              ↓
                              CDN (static JSON file, read fan-out)
```

A Redis-for-ingest design that isolates workloads would still be **two Redis
clusters** (ingest log vs CDN JSON) plus the same projector hop. That does not
remove a datastore; it replaces DynamoDB with a memory-sized ledger.

## Workloads are different

| | Ingest store | Serving Redis → CDN |
|---|---|---|
| **Job** | System of record: every event, idempotent upsert, history, CDC source | Cache of a **document**: UI-shaped JSON the client already GETs |
| **Shape** | Many small items; range queries and GSIs (game timeline, type, top-N, follow-a-team) | One key → one JSON file per game (or feed) |
| **Reads** | This API / other services / projector / rebuild | Viewers at huge QPS via CDN |
| **Loss** | Lost event = wrong product | Cache miss: rebuild from the ingest store |
| **Scale** | Grows with every league and Olympic discipline | Bounded: live + recently requested games |

The JSON on the CDN is a **projection** (score, clock, recent incidents), not a
dump of DynamoDB items. It may be overwritten, TTL'd, and rebuilt. The event
row must not.

Expected ingest volume is **large**: concurrent coverage of major football
leagues (Premier League, Bundesliga, Serie A, La Liga), NBA, MLB, and every
Olympic discipline. Event types include play-by-play (e.g. basketball scores
and fouls), not only goals. That is high write QPS and a wide keyspace, not a
handful of JSON blobs in RAM.

## Why DynamoDB works for this

DynamoDB is a fit because this product is **keyed event ingest with a short
list of known queries**, not a cache and not a general SQL warehouse. The
table is modeled for those queries up front (`keys.py` +
`football_repository.py`); DynamoDB then does what it is designed to do:
partitioned `PutItem` / `Query` at high write rate, with indexes maintained
on the write.

**The unit of work is one event, not one match document.**  
Each Kafka message becomes one item (`GameEvent.to_item()`). Items are small
(player, team, minute, timestamps — well under the 400KB item limit). The
hot path is “upsert this fact,” which is `put_item`, not read-modify-write of
a growing JSON blob. At play-by-play volume (NBA shots, MLB pitches, Olympic
disciplines in parallel) that stays a cheap, parallel write. Rewriting a
whole-match document on every incident would serialize updates per game and
grow the payload as the match progresses.

**Partitioning matches how sport is actually watched.**  
PK is `SPORT#{sport}#{competition}`; events for a game sit under that
partition with a structured SK (`GAMEID#{id}#EVENT#{type}#{event_id}`).
Kafka already keys `{competition}#{game_id}`. Concurrent Premier League,
NBA, and Olympic sessions are many partitions, not one global lock. Capacity
scales with live matches. On-demand billing matches the calendar: quiet
midweek, Saturday 15:00 kickoffs, two weeks of Olympic peak.

**Access patterns are known and bounded.**  
The schema contract is the query list, not an ERD:

| Need | DynamoDB operation |
|---|---|
| Competition / game / team metadata | `GetItem` |
| Timeline for a game | `Query` PK + `SK begins_with GAMEID#…#EVENT#` |
| All goals (or one type) | Same, tighter SK prefix |
| Latest of a type | Reverse query, `Limit=1` |
| Events in a time window | `SK BETWEEN` on time-ordered ids |
| Top-N recent across types | **GSI2** (`INGESTEDAT`), `Limit=N`, descending |
| Follow a team across competitions | **GSI1** |

Each of those is one request with a partition key and a sort-key condition.
DynamoDB is built for that. The single-table layout colocates a competition’s
games, events, teams, and players so hierarchical reads are one `Query`, not
N tables joined in the app. Basketball reused the same table with a different
`SPORT#` prefix — no new datastore.

**One write fills every index the product needs.**  
GSI1 (team) and GSI2 (game × ingest time) are attributes on the same item.
The consumer does not maintain secondary keys. Kafka redelivery overwrites
the item; the indexes follow. That is how ingest stays “normalize, then
`put_item`” while still supporting follow-a-team and top-N.

**At-least-once Kafka plus overwrite is the ingest contract.**  
`event_id` is uuid5(`provider + raw payload`). PK/SK are determined by the
message. A consumer crash before offset commit retries the same item, not a
duplicate goal. Time order for bursts in the same millisecond lives in the
sort key (monotonic uuid / `INGESTEDAT#…`), so queries do not depend on
arrival order in Redis.

**This service is the system of record for an existing fan-out, not the
read path for viewers.**  
Clients GET JSON from the CDN. DynamoDB’s job is durable facts + change
capture (`DynamoDB → CDC → Kafka → serving Redis → CDN`). Streams are
per-item and line up with `put_item`. Rebuild after a Redis flush is “read
the table (or replay Kafka while retained),” not “hope AOF had the Olympics.”
Viewer QPS never hits DynamoDB; that is why on-demand ingest at league scale
stays viable.

**Latency budget has room for `PutItem`.**  
Live video is 30–45s behind. The POC target is a few seconds provider →
persisted item. DynamoDB persist is milliseconds to low hundreds of ms after
Kafka pickup — inside the budget. The store can optimize for durability,
idempotency, and query shape instead of shaving ingest microseconds.

**History and live ingest use the same mechanism.**  
Followed-team queries, late stats, projector catch-up, and “rebuild this
final’s JSON” all read the same items that live ingest wrote. DynamoDB
keeps that set on storage, not in an ingest Redis sized for Olympic peak
RAM. Serving Redis only needs the hot JSON.

**Operational shape matches a multi-sport platform.**  
One table, two GSIs, pay-per-request (POC) or provisioned/on-demand in AWS,
IAM and backups like the rest of the stack, DynamoDB Local for this repo.
Adding a sport is a key prefix and a normalizer, not a new cluster.

Caveats (hot partitions on a single final, GSI WCU, names-in-keys) are real
and listed under pros/cons; they are capacity and modeling issues on a store
that already fits the workload, not signs the workload is a cache.

## Why DynamoDB rather than Redis for ingest

### Access patterns are queries, not GET-by-key

Match Alerts access patterns (see `football_repository.py`) are bounded
`Query`s over a partition plus two GSIs on the **same** item:

- All events for a game; events of one type; latest of a type; time range
- Top-N recent events across types (**GSI2**, ingest time)
- Games/events for a followed team (**GSI1**)

`GameEvent.to_item()` writes base PK/SK plus GSI1/GSI2 attributes in one
`put_item`. DynamoDB maintains those indexes with the item.

Redis has no GSI. The same semantics need several structures per write (hash /
stream, per-game timeline, per-type index, sorted set by ingest time, per-team
index) kept consistent on Kafka retries. That reimplements DynamoDB indexes in
application code.

### Kafka is at-least-once; identity is the item key

The consumer derives a deterministic uuid5 `event_id` from provider + raw
payload. PK/SK are determined by the message. Re-delivery must **overwrite**,
not append a second goal. `put_item` is that primitive; GSIs follow the item.

Redis streams/lists append. Idempotency then needs a seen-set, Lua, or
careful `ZADD` — a ledger on top of Redis.

### Ordering is in the sort key

Bursts (goal + card in the same millisecond) use time-ordered keys (uuid /
`INGESTEDAT#…`). `Query` + `ScanIndexForward` + `Limit` serves top-N without
loading the whole game into the app (that is why GSI2 exists).

### Durable log and CDC

Downstream is **DynamoDB → CDC → Kafka → serving Redis → CDN**. DynamoDB
Streams match one item change → one downstream event. This service is done
when the item exists. Redis keyspace notifications / Redis Streams are not
that pipeline.

If serving Redis loses JSON, rebuild from DynamoDB (or Kafka, with retention
limits). Kafka will not keep a full MLB/Olympic season unless you pay to
retain it. The ingest store is long retention; serving Redis is not.

### Volume and spikes

Writes fan in per competition/game partition (Kafka already keys
`{competition}#{game_id}`). Thousands of concurrent games are thousands of
partitions. On-demand capacity matches “quiet Tuesday” vs Saturday 15:00
kickoffs vs an Olympic session.

Redis as system of record at this volume is **peak RAM × replicas** for the
full event archive (or eviction = data loss). Serving Redis stays a small hot
set of JSON files. Those sizes must not share a cluster — see below.

### Latency is not the deciding variable

The live-stream delay budget is **30–45 seconds**; ingest targets a few
seconds. Kafka pickup plus one `PutItem` is already far inside that. Redis
might save milliseconds on persist. It does not decide idempotency, indexes,
CDC, or rebuild.

## Why Redis for serving (and only there)

- **Document cache:** one key, one JSON file; no GSI required.
- **Read amplification:** match-day GETs hit the CDN, not DynamoDB. DynamoDB
  sees ingest + CDC/projector workers.
- **Freshness:** update the blob when an event arrives; between events the
  file is static.
- **User-visible latency:** sub-ms Redis / edge GET is the right
  optimization. Ingest persist time is not.

Serving Redis should be writable **only by the fan-out/projector**, so a
poller replay cannot corrupt the only copy of truth, and Redis can be flushed
and regenerated from DynamoDB.

## Two separate Redis clusters

If ingest were Redis, it would **not** share the CDN cluster. Topology:

| Cluster | Role | Working set | Policy |
|---|---|---|---|
| **Ingest Redis** | Event log and secondary indexes | All retained events (leagues + Olympics) | `noeviction`, persistence, sized for peak |
| **Serving Redis** | JSON for CDN | Live / recently requested games | TTL, eviction OK |

**What two clusters get right:** Olympic write storms do not evict `GET
game.json`. SLOs and paging can differ.

**What they do not get you:**

- They do not remove the projector. Path is still ingest store → rewrite JSON
  → serving Redis → CDN, same as DynamoDB CDC → Redis → CDN.
- They do not mean “we already run Redis, ingest into it.” Ingest is a **new**
  fleet (sizing, AOF at play-by-play QPS, shards, failover). Serving Redis
  remains. Versus DynamoDB + serving Redis you swapped DynamoDB for a
  RAM-priced ledger; you did not drop a datastore.
- Almost all cost and risk sit on the **ingest** cluster: memory for the large
  set, peak-sized for the Olympics all year. Serving Redis stays cheap.

Two clusters are the correct Redis topology. They are also evidence the
workloads must not share a cluster — which is why ingest belongs in DynamoDB
and only JSON belongs in Redis.

## Pros and cons

### DynamoDB as ingest store

**Pros**

- One `put_item` updates the item and GSI1/GSI2; consumer stays
  normalize-then-write.
- Idempotent overwrite under Kafka redelivery (uuid5 → PK/SK).
- Range queries the product actually specified; no app-side join of N Redis
  structures.
- Durable, backed up, on-demand for match-day / Olympic spikes.
- Natural CDC (Streams) into the existing Kafka → Redis → CDN path.
- Storage/WCU pricing for a **large** event set; RAM reserved for the **small**
  serving working set.
- Same table layout extends across sports via `SPORT#` prefix.
- Local DynamoDB already matches prod for this POC.

**Cons**

- Single-table key design is upfront complexity (`keys.py`, GSI attributes).
- GSI write amplification (base + two ALL-projected GSIs) shows up on the
  bill at play-by-play rates.
- Per-partition throughput: a World Cup final, El Clásico, or Olympic 100m
  can hot-key one PK / GSI2PK. Mitigate with game-level keys; avoid one
  partition for “all Olympics.”
- GSI1 `TEAM#{name}` can concentrate writes on a superstar in a final.
- `PutItem` → Streams → projector adds hop latency vs writing serving Redis
  from the consumer (usually still inside the 30–45s budget).
- This API’s monitor path (`scan_recent_events`) is a table scan; it is not
  the production read path (CDN is).
- Identifier strings in keys (competition/team **names**) do not follow
  Aurora renames; prefer stable canonical IDs (same issue in Redis).

### Redis as ingest store (including a dedicated ingest cluster)

**Pros**

- Sub-ms reads/writes; familiar GET/SET/JSON.SET.
- A single match document can be updated in Kafka partition order without a
  GSI (if the only prod read is that document).
- TTL maps cleanly to “live window” if history is disposable.
- Two-cluster split protects CDN GETs from ingest write storms.
- Dedupe is possible (uuid5 key, Lua, seen-set) — not unique to DynamoDB.
- If **nothing** ever queried DynamoDB (only CDN JSON), the table would be a
  write-only buffer; Redis advocates will call that redundant with Kafka +
  serving Redis.

**Cons**

- Not a query engine: GSI1/GSI2 become multiple structures and consistency
  bugs at high write QPS.
- Streams/lists append; idempotent ingest is extra machinery.
- System of record in RAM: size for Olympic **peak**, replicas, persistence;
  eviction is data loss.
- Persistence (AOF) at league + Olympics play-by-play rates is operational
  cost, not a checkbox.
- CDC/rebuild story is weaker than DynamoDB Streams; Kafka retention is not
  a season-long archive by default.
- Two clusters + projector ≈ DynamoDB + serving Redis in moving parts, with
  the large working set billed as memory.
- Couples ingest to frontend JSON schema if the consumer writes documents
  directly.
- Redis Streams are not a substitute for Kafka (already the ingest buffer).

### Serving Redis + CDN (keep)

**Pros**

- Right primitive for “file the client fetches.”
- Absorbs read fan-out; DynamoDB does not.
- Working set stays bounded if it only holds live/recent JSON.
- Isolated from ingest (especially as its own cluster).

**Cons**

- Must not be the only copy of events.
- Stale if the projector lags (true for any cache).
- Serving down ⇒ users see nothing even if DynamoDB is healthy — accept that;
  do not “fix” it by ingesting into the same cluster.

## Steelman: when Redis ingest could look attractive

These are the arguments worth answering, not “Redis is faster.”

1. **The user-visible store is already JSON on a CDN.** DynamoDB GSIs are unused
   by the frontend. Write the document you serve.
2. **CDC is extra latency and failure modes** vs consumer → serving Redis → CDN.
3. **Play-by-play DynamoDB + two GSIs is real WCU spend**; Redis spends RAM
   instead (not cheaper at this volume, but a different invoice).
4. **If history is not a product requirement**, TTL’d Redis plus Kafka replay
   might be enough.

They only hold if production never queries the event table, Redis flush is
acceptable except for Kafka replay, follow-a-team / range / top-N live in the
JSON or nowhere, volume stays small, and ingest and serving may share fate.

**Large multi-league + Olympics ingest, two Redis clusters, and a CDN JSON
cache reverse those assumptions.** Ingest volume and serve volume are different
shapes. DynamoDB holds the large one; serving Redis holds the small one.

## What Redis is still good for (complements, not replacements)

- Serving cluster: current match JSON for the CDN (the production read path).
- Optional: canonical “does this game_id exist?” projection on the ingest hot
  path (small, TTL after kickoff) — DynamoDB or Redis.
- Optional: demo runner / locks; rolling “recent events” window so the monitor
  can drop `Scan`.
- Not: Kafka → persist path for the event log.

## Decision

| Concern | Store |
|---|---|
| Idempotent event identity, history, team follow, top-N | DynamoDB |
| Rebuild / debug / CDC source | DynamoDB |
| Large, spiky ingest (leagues + Olympics, play-by-play) | DynamoDB |
| Current match JSON for the client | Serving Redis (separate cluster) |
| Serve that JSON at scale | CDN |
| Canonical existence check | Small projection (DynamoDB or Redis), not the CDN cache |

**Do not ingest into serving Redis.**  
**Do not add an ingest Redis cluster to avoid DynamoDB** — that pays peak RAM
for the archive and still needs a projector into serving Redis.

The frontend Redis/CDN path is already in the architecture, in the right
layer. This service’s job is a reliable DynamoDB source so that cache can
stay a cache.
