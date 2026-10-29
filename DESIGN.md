# Exchange8:VFeeds - Detailed Design Document

> Wire-level construction of the external venue feeds service. For the
> overview read `README.md`; for business functionality read `BUSINESS.md`.

## 1. Scope & Context

**Owns**

- The external venue connection surface: every market-data transport and
  payload the platform consumes from outside the vertical.
- The normalized quote-event model (`VenueQuoteEvent`) and the per-instrument
  consolidated BBO state across venues.
- The A/B sequence-recovery core for dual-leg feeds.
- Indices: definitions, constituents, computed values, value history, and the
  divisor-continuity rebalance.
- Tables in schema `vfeeds` of database `exchange8`: `external_feed`,
  `external_retrans_feed`, `previous_day_summary`, `index`,
  `index_constituent`, `index_value`, plus the generated `outbox` and
  `audit_journal`.
- Event families published under `exchange8:vfeeds:*`.

**Does NOT own**

| Concern | Owner |
|---|---|
| Publication of market data to consumers | Exchange8:MD (`services/md`) |
| Routing decisions from per-venue quotes | Exchange8:SOR (`services/sor`) |
| Order states, external best-bid/offer use | Exchange8:OMS (`services/oms`) |
| Mark-to-market, funding, Greeks pricing | Exchange8:Margin (`services/margin`) |
| Security master (`refdata.security`) | Exchange8:RefData (`services/refdata`) |
| Event storage + stream distribution | Sentin8:Bus |
| Token issuance / JWKS | Sentin8:Auth |
| Audit aggregation and decisions store | Sentin8:Sys, Sentin8:Policy |

System context:

```mermaid
graph TD
    subgraph EDGE["External venue networks - untrusted"]
        V1["TSX / TSXV / Chi-X / Aequitas / CSE - multicast, SOUP, TL1"]
        V2["Crypto venues - REST + WebSocket"]
    end
    subgraph EX8["exchange8 vertical - database exchange8"]
        VF["exchange8-vfeeds - schema vfeeds"]
        MD["exchange8-md"]
        SOR["exchange8-sor"]
        OMS["exchange8-oms"]
        MARGIN["exchange8-margin"]
        REF["exchange8-refdata - schema refdata"]
    end
    subgraph PLAT["Sentin8 platform - trust boundary"]
        AUTH["Sentin8:Auth - issuer"]
        BUS["Sentin8:Bus"]
        SYS["Sentin8:Sys - audit ingest"]
        POL["Sentin8:Policy - Shield"]
    end
    V1 -- "MulticastAB / SoupTcp / Tcp frames (unauthenticated transport, secret-ref logins)" --> VF
    V2 -- "HTTPS poll + WSS stream" --> VF
    VF -- "reads security master" --> REF
    VF -- "certificate-presenting POST /v1/produce (sentin8:bus:produce)" --> BUS
    VF -- "M2M POST /v1/audit (sentin8:sys:audit:write)" --> SYS
    AUTH -- "bearer verify + JWKS; workload certificates" --> VF
    POL -- "policy sets (scope-mirror) + hot reload" --> VF
    VF -. "consumes vfeeds events" .-> MD
    VF -. "consumes vfeeds events" .-> SOR
    VF -. "consumes vfeeds events" .-> OMS
    VF -. "consumes vfeeds events" .-> MARGIN
```

Container view:

```mermaid
graph TD
    subgraph BIN["exchange8-vfeeds binary (sentin8::shell)"]
        subgraph HTTP["HTTP surface - PQ-TLS terminated in-process"]
            HL["GET /v1/health"]
            PA["Shield admin API - mount_policy_admin"]
        end
        subgraph DOMAIN["Domain core"]
            ADPT["adapters.rs - VenueAdapter registry"]
            SEQ["recovery.rs - AbSequencer"]
            BBOE["bbo.rs - BboEngine"]
            IDX["index.rs - IndexEngine"]
            IV["adapters.rs - IvSurface"]
        end
        subgraph WORKERS["Shell boot workers (fail-open loops)"]
            RELAYW["outbox relay - sentin8_outbox::spawn_relay"]
            PUSHW["audit pusher - sentin8_audit::spawn_journal_pusher"]
            HOTREL["Shield hot-reload consumer (boot_full)"]
        end
    end
    subgraph PERSIST["database exchange8 - schema vfeeds (RLS forced)"]
        T1["external_feed"]
        T2["external_retrans_feed"]
        T3["previous_day_summary"]
        T4["index + index_constituent + index_value"]
        OBOX["outbox"]
        JRN["audit_journal"]
    end
    subgraph SVC["Runtime registrations - vertical enroll"]
        REG["registration.rs - scopes, roles, M2M grants, RFW pages, policy set"]
    end
    HTTP -- "axum Router (extend_router)" --> DOMAIN
    DOMAIN -- "domain change + outbox row (same tx)" --> OBOX
    DOMAIN -- "domain change + journal row (same tx)" --> JRN
    RELAYW -- "drains unrelayed rows" --> OBOX
    PUSHW -- "drains unpushed rows" --> JRN
    REG -- "--print-registration -> vertical enroll" --> SVC
```

## 2. Runtime Architecture

Components inside the binary:

1. **Shell boot** (`sentin8::shell::serve`): `--print-registration` flag
   before any initialization → PQ provider install → env config
   (`EXCHANGE8_VFEEDS_PORT`, `EXCHANGE8_VFEEDS_DATABASE_URL`) → capped pool
   (`sentin8_config::pg_pool::connect`, default max 3) → PostgreSQL 18+
   enforcement → Shield engine boot (`boot_full`: policy-set load against the
   live platform table; unparsable text **refuses to start**) → state build →
   router assembly → middleware stack → in-process PQ-TLS accept loop.
2. **Router extension** (`main.rs::extend_router`): the only route addition is
   `mount_policy_admin` - the Shield admin API (policy sets, relations,
   attributes, decisions, RFW POST aliases). The shell mounts `/v1/health`
   with the in-flight gauge for the drain signal. The service declares RFW
   pages `feeds` (`/v1/admin/ui/feeds`, data `/v1/admin/feeds`) and `indices`
   (`/v1/admin/ui/indices`, data `/v1/admin/indices`) through registration;
   their backing handlers are listed in §10.
3. **Domain core**: the four modules `adapters.rs`, `recovery.rs`, `bbo.rs`,
   `index.rs` are pure and unit-tested in-module; workers and sockets wrap
   them. `bbo.rs` uses `exchange8::fixed::Price` (raw scale-4 fixed point).
4. **Boot workers** (shell-owned, fail-open): the outbox relay (drains
   `vfeeds.outbox` on `pg_notify('vfeeds_outbox_changed')` + a 5 s safety
   poll, pushes to Sentin8:Bus), the audit-journal pusher (drains
   `vfeeds.audit_journal` to Sentin8:Sys `POST /v1/audit`), and the Shield
   hot-reload direct consumer.
5. **Feed sessions** (ownership contract, `adapters.rs` docs): dedicated
   per-feed tasks own transports, SOUP login/sequenced/heartbeat, multicast
   joins, reconnection and backoff; they feed the A/B sequencer and adapters
   (spawn site noted in §10). Panics never cross feeds - task isolation keeps
   other venues streaming.

### Request family - venue feed connect / consume

```mermaid
sequenceDiagram
    participant Venue as External venue (A/B legs)
    participant Task as Feed session task
    participant Seq as AbSequencer
    participant Ad as VenueAdapter
    participant BBO as BboEngine
    participant DB as vfeeds schema (PostgreSQL)

    Task->>Venue: connect transport (SOUP login / mcast join / TCP read)
    Venue-->>Task: framed message carrying venue seq
    Task->>Seq: on_message(FeedMessage{feed: A|B, seq, payload})
    alt seq equal next expected
        Seq-->>Task: ready (in order)
    else seq below next expected
        Seq-->>Task: stale - dropped (dedup)
    else seq above next expected
        Seq-->>Task: buffered - FeedHealth::Recovering
        Task->>Seq: on_b_wait_timeout() after B-fill window
        Seq-->>Task: RetransmitRequest{from_seq,to_seq,count}
        Task->>Venue: request range from recovery endpoint (external_retrans_feed)
        Venue-->>Task: retransmitted messages close the request
    end
    Task->>Seq: take_ready()
    Seq-->>Task: in-order payloads only
    Task->>Ad: parse(frame)
    Ad-->>Task: Vec<VenueQuoteEvent>
    Task->>BBO: on_quote(venue_index, bid, ask)
    alt CBBO changed
        BBO-->>Task: QuoteUpdate::Changed(Bbo)
    else crossing state detected (bid > ask)
        BBO-->>Task: QuoteUpdate::CrossZeroed(Bbo) + cross_alerts += 1
    else identical quote
        BBO-->>Task: QuoteUpdate::Unchanged (no publication)
    end
```

### Request family - tick / BBO fan-out to Exchange8:MD

```mermaid
sequenceDiagram
    participant BBO as BboEngine
    participant W as Domain writer (worker)
    participant PG as Postgres (db exchange8)
    participant R as Shell outbox relay
    participant BUS as Sentin8:Bus
    participant MD as Exchange8:MD (direct consumer)

    BBO-->>W: QuoteUpdate::Changed(Bbo)
    W->>PG: BEGIN, domain row + INSERT vfeeds.outbox (event_type, payload, idempotency_key = row id), COMMIT
    PG-->>R: pg_notify('vfeeds_outbox_changed')
    R->>BUS: POST /v1/produce (M2M token, scope sentin8:bus:produce)
    BUS-->>MD: bus:{env}:{family} stream entry
    MD->>BUS: consume + POST /v1/ack (bus-client)
    Note over BBO,MD: Relay fail-open: Bus outage leaves rows relayed=false, 5 s poll retries
```

### Request family - index computation

```mermaid
sequenceDiagram
    participant SRC as Constituent source (adapters IndexTick stream)
    participant IDX as IndexEngine
    participant VAL as vfeeds.index / index_constituent (Postgres)
    participant W as Domain writer
    participant MGN as Exchange8:Margin (event consumer)

    W->>VAL: load index definition (method, divisor) + constituents (weights)
    W->>IDX: compute(ConstituentTick[..])
    IDX->>IDX: 1. aggregate: median | volume-weighted | time-weighted
    IDX->>IDX: 2. outlier rejection <= outlier_bps from median, recompute once
    IDX->>IDX: 3. value = aggregate / divisor
    IDX-->>W: Some(value) (None = no usable constituent)
    W->>VAL: INSERT vfeeds.index_value (history for audit + settlement)
    W-->>MGN: index:updated event (outbox -> Bus -> consumer group)
    Note over IDX: rebalance_divisor(new_raw, old_value) keeps the published level continuous on constituent changes
```

### Request family - recovery / replay

```mermaid
sequenceDiagram
    participant A as Feed A (primary)
    participant B as Feed B (backup)
    participant Seq as AbSequencer
    participant RQ as Venue recovery endpoint
    participant Sink as Adapter / CBBO path

    A-->>Seq: seq 1 (in order, delivered)
    A-->>Seq: seq 4 (hole 2..3 → buffered)
    Seq-->>Sink: nothing yet - health = Recovering
    B-->>Seq: seq 2, seq 3 (B fills the hole)
    Seq-->>Sink: 2, 3, 4 in order - Healthy
    Note over Seq: staleness: B replay of already-delivered seq is dropped
    A-->>Seq: seq 9 (hole 5..8)
    Seq-->>Seq: on_b_wait_timeout → request 5..8
    Seq->>RQ: RetransmitRequest (bounded by buffer_cap backstop)
    RQ-->>Seq: retransmissions (either leg) close pending_request
    Seq-->>Sink: 5..9 in order
```

## 3. Data Model

Schema `vfeeds` in database `exchange8`; every table is tenant-scoped and
RLS-enforced. Role `exchange8_vfeeds_service` (no `BYPASSRLS`) gets DML grants
scoped to this schema; the service's DSN is `EXCHANGE8_VFEEDS_DATABASE_URL`
(user `exchange8_vfeeds_service`, OAuth token auth via `vertical-registry.json`).

```mermaid
erDiagram
    EXTERNAL_FEED ||--o{ EXTERNAL_RETRANS_FEED : "recovery endpoints"
    INDEX ||--o{ INDEX_CONSTITUENT : "constituents"
    INDEX ||--o{ INDEX_VALUE : "published values"

    EXTERNAL_FEED {
        uuid id PK
        uuid env_id
        text adapter "CHECK in adapter set"
        text transport
        jsonb params "transport address/login params"
        text creds_secret_ref "Sentin8:Secrets reference only"
        time open_time
        time close_time
        boolean enabled
    }
    EXTERNAL_RETRANS_FEED {
        uuid id PK
        uuid env_id
        uuid feed_id FK
        text address
        integer port
        integer timeout_ms
    }
    PREVIOUS_DAY_SUMMARY {
        uuid id PK
        uuid env_id
        uuid security_id
        date trade_date
        numeric_18_8 close_price
        numeric_18_8 volume
        text lot_policy "board|odd|all"
    }
    INDEX {
        uuid id PK
        uuid env_id
        text name
        text method "median|volume_weighted|time_weighted"
        numeric_20_8 divisor
    }
    INDEX_CONSTITUENT {
        uuid id PK
        uuid env_id
        uuid index_id FK
        uuid security_id
        text venue_mic
        numeric_18_8 weight
    }
    INDEX_VALUE {
        uuid id PK
        uuid env_id
        uuid index_id FK
        numeric_20_8 value
        timestamptz ts
    }
    OUTBOX {
        uuid id PK
        uuid env_id
        text event_type
        text stream_family
        jsonb payload
        boolean relayed
        int relay_attempts
    }
    AUDIT_JOURNAL {
        uuid id PK
        uuid env_id
        text action
        text actor_user_id
        jsonb before_state
        jsonb after_state
        boolean pushed
    }
```

Per-table notes:

| Table | Ownership / visibility |
|---|---|
| `vfeeds.external_feed` | Tenant config. Adapter CHECK constraint is the compile-time registry (`stamp_cbbo`, `stamp_quotes`, `itch_orders`, `xmt`, `soup_tcp`, `mold_udp`, `fix`, `websocket`). `creds_secret_ref` is a Sentin8:Secrets reference - never plaintext. Select/insert/update/delete policies per operation. |
| `vfeeds.external_retrans_feed` | Per-feed retransmission endpoint (address, port, `timeout_ms` default 5000). FK to the owning feed. |
| `vfeeds.previous_day_summary` | One row per `(env_id, security_id, trade_date)`: close price, volume, lot policy (`board`/`odd`/`all`). Lot-policy source for gateway lot-size events. |
| `vfeeds.index` | Index definition: unique name per env; method CHECK (`median`, `volume_weighted`, `time_weighted` - the `IndexMethod` vocabulary the engine implements); divisor default 1.0. |
| `vfeeds.index_constituent` | Constituent rows `(index_id, security_id, venue_mic)` unique; `weight` default 1.0. Optional per-constituent venue for per-venue constituent indices. |
| `vfeeds.index_value` | Append-only published history; index `(env_id, index_id, ts)` for range reads. |
| `vfeeds.outbox` | Generated (`sentin8_outbox::outbox_table_sql("vfeeds")`). `relayed`/`relay_attempts` are relay state; partial index on unrelayed rows; `pg_notify` trigger `vfeeds.vfeeds_notify_outbox()` on insert. |
| `vfeeds.audit_journal` | Generated (`sentin8_audit::audit_journal_sql("vfeeds")`). Single `audit_journal_dml` policy; partial index on unpushed rows. |

RLS: every table carries `ENABLE` + `FORCE ROW LEVEL SECURITY` with policies
on the platform helpers `sentin8_context_initialized()` and
`sentin8_current_env_id()`; operations outside set context see zero rows
(silent enforcement, not an error).

## 4. API Surface

Audience groups and gates. Handler actions follow the scope-mirror policy set
(`registration.rs::SCOPE_MIRROR_POLICIES`); the registered error code is
`exchange8_feed_gap_detected` (HTTP 503, enum `Exchange8FeedGapDetected`).

**Operational / infrastructure**

| Endpoint | Gate | Purpose |
|---|---|---|
| `GET /v1/health` | none (shell) | Liveness + `build` + `active_connections` for captain drain |
| `GET /openapi.json` | none (middleware-free) | Shell-merged OpenAPI |

**Shield policy admin (mounted via `mount_policy_admin`; scope
`exchange8:vfeeds:policy:read` / `:write`; admin role `exchange8:vfeeds:admin`)**

| Endpoint | Gate | Purpose |
|---|---|---|
| `GET /v1/admin/policy-sets`, `GET /v1/admin/policy-sets/{name}` | policy read | List/get policy sets |
| `PUT /v1/admin/policy-sets/{name}`, `POST /v1/admin/policy-sets`, `POST /v1/admin/policy-sets/{name}/save` | policy write | Upsert/create/save sets |
| `DELETE /v1/admin/policy-sets/{name}` | policy write | Remove a set |
| `POST /v1/admin/policy-sets/{name}/test` | policy write | Dry-run authoring |
| `POST /v1/admin/policy-sets/enforce` / `shadow` / `disable` | policy write + RFW POST alias | Mode flips |
| `POST /v1/admin/policy-sets/{name}/{action}` | policy write + RFW POST alias | Row action |
| `GET /v1/admin/policy-decisions` | policy read | Decision history |
| `GET/PUT/DELETE /v1/admin/relations`, `GET /v1/admin/relations/check`, `GET/PUT/DELETE /v1/admin/attributes` | policy write | ReBAC graph admin |

**Admin / RFW (declared in registration; handlers in §10)**

| Page | UI / data endpoints | Gate |
|---|---|---|
| `feeds` | `/v1/admin/ui/feeds` + `/v1/admin/feeds` | `exchange8:feed:read` (mutating ops `exchange8:feed:write`) |
| `indices` | `/v1/admin/ui/indices` + `/v1/admin/indices` | `exchange8:index:read` (mutating ops `exchange8:index:write`) |
| Quote clear | `POST /v1/admin/quotes/clear` (side `bid`/`ask`/`both`) | `exchange8:feed:write` |

**Emitted events** (`exchange8:{service}:{resource}:{operation}`, outbox →
Bus; `idempotency_key` = outbox row id)

| Event | When |
|---|---|
| `exchange8:vfeeds:venue:quote` | Normalized venue quote event |
| `exchange8:vfeeds:bbo:updated` | CBBO change (source MICs carried) |
| `exchange8:vfeeds:feed:status` | Feed up/down/degraded, sequence gaps |
| `exchange8:vfeeds:index:updated` | Index recompute published |

Multi-party flow (external venue → vfeeds → Exchange8:MD):

```mermaid
sequenceDiagram
    participant V as Venue (STAMP / ITCH / TL1 feed)
    participant VF as exchange8-vfeeds
    participant OB as vfeeds.outbox
    participant BUS as Sentin8:Bus
    participant MD as exchange8-md

    V-->>VF: transport frame (multicast / SOUP / TCP)
    VF->>VF: AbSequencer ordering → adapter parse → BboEngine
    VF->>OB: INSERT outbox row (venue:quote / bbo:updated) same tx as domain write
    OB-->>BUS: relay: POST /v1/produce (M2M, sentin8:bus:produce)
    BUS->>BUS: persist inbox → XADD bus:{env}:{family}
    MD->>BUS: consume (direct consumer, sentin8:bus:consume) + POST /v1/ack
    Note over MD: re-publication to clients is Exchange8:MD's responsibility
```

Event-flow (outbox → Bus → trigger):

```mermaid
sequenceDiagram
    participant W as Domain change (vfeeds)
    participant OB as vfeeds.outbox
    participant R as Relay task
    participant BUS as Sentin8:Bus POST /v1/produce
    participant C as Consumer group

    W->>OB: INSERT row + pg_notify('vfeeds_outbox_changed')
    R->>OB: wake on notify + 5 s poll (relayed = false)
    R->>BUS: produce (idempotency_key = row id)
    alt produce success
        BUS-->>R: accepted
        R->>OB: row relayed = true, relayed_at
    else Bus outage / certificate not ready
        BUS-->>R: unavailable (fail-open)
        R->>OB: relay_attempts += 1, row stays unrelayed, retried
    else consumer-side failure
        BUS->>C: redelivery until ACK, DLQ after retries (Bus-owned)
    end
```

## 5. Cross-Cutting Concerns

**Authentication.** Sentin8:Auth is the only issuer. The shell middleware
stack (correlation, host context, TLS context, request logging, traffic
counter, CORS) front every route; inbound bearer tokens verify against the
Auth issuer (discovery + JWKS behind a shared client). Outbound clients
present the workload certificate (CERTFREE-4V - `ensure_workload_certificate`
behind `BusClient::for_workload`); a certificate-ensure failure
degrades only the fail-open relay/pusher paths, never the authorization path.

**Authorization (Shield).** The shell boots the engine (`boot_full`) from the
service's declared policy sets; unparsable text refuses to start. The seeded
`scope-mirror` set (`origin='platform'`, `mode='enforced'`) is the exact
mirror of handler gates over the LIVE effective scope: permits for
`exchange8:vfeeds:stream`, plus `sentin8:sys:admin` / `sentin8:sys:org:admin`
/ `sentin8:sys:env:admin` platform-admin permits. Step-5 decisions are
fail-closed (no set → deny; enforced no-match → deny; shadow never blocks;
forbid beats permit; M2M parity). Hot reload runs as a direct consumer of the
policy-change stream (`exchange8:vfeeds-policy` workload, `sentin8:bus:consume`).

**RLS context.** Tenant tables demand `SELECT sentin8_set_tenant_context($1)`
on the SAME connection/transaction as any data query (transaction-local
`set_config(..., true)`); the async helper must be awaited. Policies use
`sentin8_context_initialized()` + `sentin8_current_env_id()`, so unset context
sees and writes nothing.

**Idempotency & exactly-once.** Outbox rows carry `idempotency_key` (row id);
Bus dedupes on it. At-least-once Valkey delivery + idempotent consumers =
effectively-once. Stale retransmitted venue messages are dropped by the
sequencer, so replay never double-applies quote state.

**Rate limiting.** No route-level limiter in this binary; the venue rate
itself is naturally capped by transport (feed `enabled` flag, session health
events). Shield's decision history provides post-hoc analysis of admin-surface
call patterns.

**Audit journaling.** Auditable changes write `vfeeds.audit_journal` in the
same transaction as the domain change; the shell pusher (`vfeeds-audit`
workload, `sentin8:sys:audit:write`) drains to Sentin8:Sys `POST /v1/audit`.
A Sys outage delays, never drops, audit.

**Graceful drain.** `/v1/health` exposes `active_connections` from the
traffic counter; captain drains against the gauge during rolling upgrades.
Feed-session reconnection on upgrade is a new instance's session setup - no
durable feed state lives in the old process.

**TLS posture.** In-process PQ-TLS termination (`sentin8-tls`); the TLS
library is database-free - host context resolution rides Sys
`GET /v1/resolve/hostname` via the `exchange8-vfeeds-tls` workload (scope
`sentin8:sys:instances:read`) with a TTL cache, no DSN, no platform grants
. Never `danger_accept_invalid_certs`; credentials never ride the
wire in plaintext (secret references at rest, Sentin8:Secrets at fetch).

## 6. Failure Modes & Recovery

```mermaid
flowchart TD
    GAP["Sequence gap on leg A"] --> BUF["Buffer out-of-order messages (bounded)"]
    BUF --> HEALTH["FeedHealth::Recovering"]
    BUF --> BWAIT{"B leg fills within window?"}
    BWAIT -- "yes" --> DRAIN["Drain in order - Healthy"]
    BWAIT -- "no (on_b_wait_timeout)" --> REQ["RetransmitRequest{from_seq, to_seq, count}"]
    REQ --> RETR{"Venue recovery endpoint answers? (external_retrans_feed)"}
    RETR -- "yes" --> FILL["Retransmissions close pending_request → drain → Healthy"]
    RETR -- "no / request unsatisfied" --> BACKSTOP{"buffer_cap exceeded?"}
    BACKSTOP -- "yes" --> REOPEN["Re-open request (covers hole to highest seen)"]
    BACKSTOP -- "no" --> REQ
    REOPEN --> REQ
    REQ -- "unrecoverable persistence" --> RESET["Gap error surface: exchange8_feed_gap_detected (HTTP 503) + feed:status degraded"]
```

```mermaid
sequenceDiagram
    participant F as Venue feed (down / gapped)
    participant T as Feed session task
    participant R as Venue recovery endpoint (external_retrans_feed)
    participant Q as Adapter / CBBO state
    participant E as Consumers (MD / SOR / OMS)

    F-->>T: silence or gap
    T->>Q: stale-drop + buffering window
    T->>R: retransmission request (timeout_ms)
    alt recovery succeeds
        R-->>T: missing range
        T->>Q: in-order drain → CBBO recomputed
        Q-->>E: bbo:updated (current best, MICs marked)
    else recovery fails
        T-->>Q: venue quotes reset (affected venue only)
        Q-->>E: feed:status degraded + bbo recomputed without the venue
    Note over F,E: consumers act on staleness policy, other venues unaffected
    end
```

Other durable failures:

- **Relay→Bus outage**: rows stay `relayed = false`; relay re-opens with
  the workload certificate when Bus returns; the 5 s poll is the backstop.
  Nothing is lost in the service's database, and the service keeps serving.
- **Journal→Sys outage**: unpushed rows accumulate; pusher retries on its
  safety pass. Audit never drops.
- **PostgreSQL failure at boot**: pool connect or PG 18 check failure aborts
  startup (captain restarts the instance). Pool saturation is bounded by the
  capped pool (default 3).
- **Policy-set load failure at boot**: refuse-to-start (fail-closed). Shield
  hot reload failing at runtime keeps the last good engine loaded.
- **Adapter panic**: the per-feed task restarts with backoff; sibling feeds
  continue; a `feed:status` event marks the affected feed.
- **DLQ**: consumer-side trigger failures land in Sentin8:Bus's DLQ after
  retries for manual replay - never silently dropped.

## 7. Concurrency & Performance Model

```mermaid
graph LR
    subgraph FEEDS["Per-feed session tasks (isolation: one failure never crosses feeds)"]
        T1["TSX STAMP CBBO task"]
        T2["ITCH orders task"]
        T3["TL1 / XMT / UDP2IP tasks"]
        T4["Crypto REST poller"]
        T5["Crypto WS task"]
    end
    SEQ["AbSequencer (per session; BTreeMap buffer, buffer_cap)"]
    ADPT["Adapter parse (pure, per frame)"]
    BBO["BboEngine per instrument (Vec VenueQuote + last Bbo)"]
    IDX["IndexEngine per index (method + divisor + last value)"]
    CH["mpsc to domain writer"]
    PG[("Postgres: vfeeds schema - pool cap = SENTIN8_PG_POOL_MAX (default 3)")]
    RELAY[("relay task - pg_notify + 5 s poll")]
    PUSH[("audit pusher - 5 s pass")]
    HOT["Shield hot-reload consumer"]

    T1 --> SEQ --> ADPT --> CH
    T3 --> SEQ
    T4 --> ADPT
    T5 --> ADPT
    ADPT --> BBO
    ADPT --> IDX
    CH --> PG
    PG --> RELAY
    PG --> PUSH
    HOT -- "bus:policy-changes (consume)" --> HOT
```

- **Task layout.** One dedicated task per feed config row (transport I/O,
  heartbeat, reconnect); workers for relay, audit pusher, Shield hot reload;
  the axum accept loop runs under `tls.serve`.
- **Locking.** Per-instrument engine state is owned by the consuming task -
  no shared mutex on the quote hot path; cross-task handoff is an mpsc
  hand-off to the single writer that performs transactional domain writes.
- **Hot paths.** CBBO recompute is a linear scan over a small dense venue
  array with early `Option` skips (design budget < 50 µs per update); adapter
  parse is branch-light, allocation-light for the binary adapters.
- **Backpressure.** Bounded buffer (`buffer_cap`) converts venue stall into
  retransmission demand instead of unbounded memory; the DB pool cap turns
  write pressure into queued outbox rows rather than pool exhaustion.
- **Metrics.** `active_connections` gauge via the shell traffic counter;
  cross-alert counter (`cross_alerts`) on the CBBO engine; per-feed health in
  `feed:status` events.

## 8. Invariants, Insecure-Defaults Rejected, Unsafe Rationale

**Invariants (always true)**

1. `AbSequencer` never delivers out of order: delivery requires
   `seq == next_seq`; buffer drain always preserves ascending sequence.
2. Stale (`seq < next_seq`) venue messages are dropped - replay from either
   leg never re-applies delivered state.
3. `BboEngine` consolidated sides consider only non-empty venue quotes; a
   venue with no quote can never win a best scan.
4. A crossed consolidated state triggers zeroing of the crossing venue's
   quote, an alert increment, and never publishes the crossed pair.
5. Index publication is `None` when the divisor is non-positive or no usable
   constituent remains; the divisor only changes via `rebalance_divisor`,
   which keeps the published level continuous.
6. Domain change and its `vfeeds.outbox` / `vfeeds.audit_journal` row commit
   in the same transaction.
7. Every RLS-protected table is `ENABLE` + `FORCE`d; the service role is
   granted only on schema `vfeeds` (plus the declared `SELECT` on
   `refdata.security`).
8. `creds_secret_ref` never contains plaintext credentials; adapters receive
   resolved or referenced credentials only at session setup.
9. Shield is fail-closed: no loaded set denies; enforced no-match denies;
   shadow never blocks; forbid dominates permit; `token_tp` never bypasses.

**Insecure defaults rejected**

- Dense zero-filled venue arrays with best-scan zero-inclusion (an empty venue
  could win the "best ask" scan) - replaced by `Option`-based storage.
- Plaintext credential columns on feed rows - secret references only.
- `BYPASSRLS` on any service role - rejected platform-wide.
- Emitting quotes as raw venue payloads to consumers - only the normalized
  event model crosses the boundary.
- Startup with unparsable policy sets - refuse-to-start instead of
  running unguarded.

**Unsafe rationale.** The crate contains no `unsafe` blocks. PQ-TLS and
crypto provider setup are owned by the platform shell (`sentin8-tls`), not by
this service.

## 9. Testing Strategy

```mermaid
graph TD
    subgraph UNIT["Unit - in-binary, pure cores"]
        AD["adapters.rs tests - parser fixtures per venue: ITCH, XMT binary, Aequitas framing incl. truncated, TL1, crypto JSON, unknown-line skip"]
        BBO2["bbo.rs tests - best/max selection, empty-venue exclusion, unchanged collapse, cross zero + alert, clear bid/both, unknown venue no-op, one-sided quotes, out-of-range venue, wide-cross containment"]
        IDX2["index.rs tests - median robustness incl. even counts, volume- and time-weighted, divisor scaling, outlier rejection + all-rejected fallback, rebalance continuity, degenerate inputs"]
        RECOV["recovery.rs tests - in-order delivery, stale dedup, B-fill, timeout request, retransmit close, buffer-cap backstop, order preservation"]
    end
    subgraph POLICYTESTS["Policy wiring - tests/policy.rs"]
        PL["Real scope-mirror seed: no-set deny, no-match deny, shadow passthrough, mirror coverage of every handler action, M2M parity, admin scopes, forbid-vs-permit, role-keyed tenant sets, cross-env isolation, enroll dry-run"]
    end
    subgraph SQLTESTS["SQL integration - tests/sql_matrix.rs (live Postgres, probe role, rolled-back tx)"]
        SQL["Every init.sql table FORCE RLS + select policy (self-maintaining from DDL); no-context zero rows; unauthenticated insert rejected; own-env exactly once; cross-env zero"]
    end
    REPLAY["Replay corpora - recorded venue captures through the same pure parsers (venue-feeds design §10)"]
    UNIT --> REPLAY
    POLICYTESTS --> UNIT
    SQLTESTS --> UNIT
```

- **Unit/pure**: every adapter parser, CBBO rules, index math, A/B recovery -
  deterministic, offline (`cargo test` needs no DB; integration tests skip
  silently offline).
- **Policy wiring**: `tests/policy.rs` loads the *real* seeded policy text via
  `registration::SCOPE_MIRROR_POLICIES` and proves the fail-closed/delegated
  semantics with hand-built rows (no DB).
- **RLS matrix**: `tests/sql_matrix.rs` connects as the `exchange8_probe`
  non-bypass role and derives its assertions from this service's own
  `init.sql`; all fixture work rides one rolled-back transaction.
- **Not tested here** (platform level): token issuance/verification, Bus
  broker internals, captain drain mechanics, cross-service E2E flows - covered
  by the platform's testing strategy and sibling services.

## 10. Open Questions

- **Feed-session spawn site**: the per-feed transport task (SOUP login,
  heartbeat, multicast join, REST/WebSocket loops) and its reconnect-backoff
  policy are the adapter framework's ownership contract in `adapters.rs`, but
  the spawn site (state build vs. router extension) is not yet wired in
  `main.rs`; the in-binary modules ship as pure cores today.
- **Event emission sites**: the declared event families (`venue:quote`,
  `bbo:updated`, `feed:status`, `index:updated`) need their outbox write sites
  on the engine and writer paths; no `event_triggers` are declared in
  registration yet.
- **Admin data endpoints**: `Feeds` and `Indices` RFW pages are registered,
  and the design names quote-clear and feed enable/disable; the backing
  handlers (including the `bid|ask|both` clear surface) are the remaining
  wiring.
- **Index-method vocabulary drift**: the `vfeeds.index.method` CHECK and the
  `IndexMethod` engine enum must stay the same vocabulary (`median`,
  `volume_weighted`, `time_weighted`) - a schema CHECK naming methods the engine
  does not implement would strand any row configured with them. The alignment is
  maintained by the engine's own serialization tests.
- **Index publication cadence**: tick-driven vs periodic index snapshots, and
  where `previous_day_summary` lot-policy events to gateways sit on that path
  (design-only facts not observable in this unit's sources).
- **Venue matrix completeness**: `vfeeds.external_feed.adapter` CHECK accepts
  the adapter names above, while the venue-feeds design also lists
  `alpha_l1` and Aequitas order/depth book types (LIT/NEO/DARK) on the
  adapter registry; reconciliation of the compile-time set with the CHECK list
  is outstanding.
- **Metrics surface**: cross alerts are counted in-process; the metric
  emission shape (`vfeeds_bbo_cross_alerts_total`, per-feed msg/s, gap counts,
  CBBO latency) lives with the platform stat emitter's semantics.

---

**Architecture & Design:** exchange8:vfeeds and its underlying concepts were designed by [Hamid Jahanzad](https://www.linkedin.com/in/hamidjahanzad/).
