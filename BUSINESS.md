# BUSINESS.md: Ex8:VFeeds — Business Requirements & Functionality

> Audience: product owners, business analysts, and anyone evaluating what
> Ex8:VFeeds **does for the business**. For architecture read `README.md`;
> for wire-level detail read `DESIGN.md`.

## 1. Business Purpose

- Give the exchange a complete, always-accurate picture of the outside
  market: every quoted price across every connected venue, normalized and
  consolidated, so trading decisions are made on one truth rather than on
  per-venue snapshots assembled ad hoc.
- Protect downstream systems from venue-data defects — crossed books, missing
  messages, stale quotes — by containing and recovering them inside the feed
  service rather than letting them surface in routing, margin, or client
  displays.
- Provide the dependable index values that margin, funding, and settlement
  calculations legally depend on, with protection against single-constituent
  distortion and continuity across index changes.
- Keep external market-data integration cheap to grow: adding a venue is a
  configuration and adapter matter, not a rebuild of consumption systems.

## 2. Business Scope (What the Service Provides)

| Capability | What the business gets |
|---|---|
| Managed connections to TSX (STAMP/eXMT), TMX Select, Chi-X, Aequitas, CSE-style feeds and crypto REST/WebSocket venues, with enable/disable, schedules (open/close times), and per-feed parameters. |
| Normalized market events | A single internal quote model (best bid/ask, depth, order book, venue status) regardless of vendor wire format. |
| Consolidated BBO (CBBO) | The best bid and ask across all venues per instrument, with the contributing venue identified, empty venues never counted, and crossed states contained. |
| Crossed-market containment | Automatic zeroing of a crossed consolidated quote and an alert rather than publishing corrupted top-of-book. |
| Stale-quote clearing | Administrative clearing of one venue's bid, ask, or both sides when a venue leaves a stale quote behind. |
| Feed sequence recovery | Dual-feed (A/B) consumption, dedup, gap buffering, backup-leg fill, and retransmission requests to venue recovery endpoints. |
| Index calculation | Named indices computed from constituents with median / volume-weighted / time-weighted methods and outlier rejection. |
| Index continuity | Divisor rebalance so an index never jumps when its constituent set changes. |
| Index history | Published index values retained for audit, settlement, and analysis. |
| Previous-day summary | Close price, volume, and lot policy per security per trade date as the daily reference the gateways and quote paths need. |
| Implied-volatility surface | Per-instrument IV points with interpolation, as the pricing input for options Greeks. |
| Admin surfaces | Shield policy administration over the service's own authorization surface; registered admin pages "Feeds" and "Indices". |
| Event publication | Market events published to the platform event bus for the rest of the vertical. |

**What the service does NOT own (business boundary)**

| Concern | Owner |
|---|---|
| Publishing market data to client-facing channels | Ex8:MD |
| Choosing where to route orders using per-venue quotes | Ex8:SOR |
| Order lifecycle and use of the external best quote in order handling | Ex8:OMS |
| Margin, funding rates, and Greeks pricing that consume the index and IV surface | Ex8:Margin |
| The security master referenced by constituents and summaries | Ex8:RefData |
| Payment/funds movement arising from index-linked obligations | Ex8:Settlement |
| Where published events are stored and how streams are distributed | Sentin8:Bus |
| Who may do what (authorization decisions, role resolution) | Shield (Sentin8:Policy) with Sentin8:Auth |
| Long-run audit archive and decision history | Sentin8:Sys |

## 3. Business Capabilities

**Feed management**

| Capability | Business rule |
|---|---|
| Feed onboarding | A venue feed is one configuration row: adapter type, transport, parameters, credential reference, and open/close schedule; enabling and disabling is an administrative decision with immediate effect on sessions. |
| Credential protection | Venue logins are stored only as secret references; no operator, developer, or database reader can see plaintext credentials. |
| Multiple transports per venue | A venue may appear with more than one feed type (e.g. consolidated CBBO plus individual L1) — the authoritative consolidated quote path is the one configured as such. |
| Recovery endpoints | Feeds that support venue-side retransmission declare their recovery host/port and timeout with the feed. |

**Consolidated market state**

| Capability | Business rule |
|---|---|
| CBBO accuracy | The consolidated best bid is the highest non-empty bid across venues; the consolidated best ask is the lowest non-empty ask; empty venues never participate. |
| Consolidation change discipline | Identical quote updates do not re-publish the CBBO; only genuine consolidated changes emit updates. |
| Crossed-market rule | When the consolidated best bid exceeds the consolidated best ask, the crossing venue's quote is zeroed, the CBBO is recomputed without it, and an alert is recorded — the crossed state is never published. |
| Quote hygiene | Operators can clear a stale quote per venue, per side, or both; the CBBO recomputes. |
| Lot policy | Previous-day close data carries the lot policy (board / odd / all) used by quote and gateway paths for odd-lot handling. |

**Market-data normalization**

| Capability | Business rule |
|---|---|
| One event model | Every venue's quote — TSX stamps, Chi-X ITCH depth adds/execs, TSX/TSXV binary L1, Aequitas frames, CSE TL1 text, crypto REST/WS payloads — becomes the same normalized event with venue identity and both venue and receive timestamps. |
| Dual-clock accounting | Venue timestamps and receive timestamps travel together so late or skewed ticks are measurable and orderable. |
| Per-venue attribution | Every quote and every consolidated side carries the venue it came from. |

**Recovery**

| Capability | Business rule |
|---|---|
| Dual-feed precedence | The A leg is primary; the B leg fills any A gap before any retransmission is requested. |
| Ordered recovery | Missing sequences are buffered, dedup'ed, and delivered in strict order; replayed (already-delivered) messages are always ignored. |
| Retransmission governance | When the backup leg does not close a gap in the bounded wait, exactly the missing range is requested from the venue's recovery endpoint; an unbounded gap backstops into a forced request rather than unbounded memory. |
| Isolation of failures | A failing venue feed never stops the others; the affected venue's quotes drop back into staleness handling while consolidated state continues from the remaining venues. |

**Index engine**

| Capability | Business rule |
|---|---|
| Index methods | An index is computed by median (robust by construction), volume-weighted (volume × weight in the window), or time-weighted (configured weights) aggregation. |
| Outlier protection | When enabled, constituents deviating more than the configured basis-point tolerance from the median are dropped and the aggregate recomputed once so no single distorted constituent steers the index. |
| Continuity on rebalance | When constituents change, only the divisor is adjusted — the published index level never jumps because of the change. |
| History | Every published index value is retained with its timestamp for audit and settlement. |

## 4. Business Events (Outbound)

| Event | When it fires | Business meaning |
|---|---|---|
| `exchange8:vfeeds:venue:quote` | A venue's normalized quote event | A specific venue's price moved |
| `exchange8:vfeeds:bbo:updated` | The consolidated best bid/offer changed | The cross-venue top of book changed — re-publication and routing refresh |
| `exchange8:vfeeds:feed:status` | Feed up/down/degraded, sequence gaps | Operations health and staleness handling for that venue |
| `exchange8:vfeeds:index:updated` | An index computed and published a new value | The reference price for margin/funding/settlement moved |

Inbound (consumed): Shield policy-change stream (hot reload of the
service's authorization surface) via the platform consumption path.

## 5. Business Rules & Invariants

1. The consolidated book is always computed from non-empty venue quotes only.
2. A crossed consolidated top of book is never published; it is contained
   (zeroed at the crossing venue) and alerted.
3. A repeated identical quote never re-publishes the CBBO.
4. Venue messages are delivered to pricing state strictly in sequence order;
   delayed or replayed duplicates from either leg are discarded.
5. An unrecoverable feed gap surfaces as an explicit degraded/gap condition —
   it is never silently papered over.
6. Index values are only published from a positive divisor and at least one
   usable constituent; outlier rejection keeps single-constituent distortion
   out of the reference price.
7. The published index level is continuous across constituent-set changes —
   only the divisor moves, not the level.
8. Close price, volume, and lot policy for a security on a trade date are
   unique per environment and never ambiguous.
9. Venue credentials exist only as secret references.
10. Every auditable change leaves a same-transaction audit record before any
    event leaves the service.

## 6. Target-User Roles & Use Cases

| Persona | What they do with the service |
|---|---|
| Exchange operations trader | Clears a stale venue quote; watches feed health and cross alerts during the trading day. |
| Venue integration engineer | Onboards or tunes a venue feed (adapter, transport, recovery endpoint) and verifies normalized output against recorded captures. |
| Trading-system product owner | Decides which venues feed the consolidated book and how feeds are scheduled per environment. |
| Index/funding product manager | Defines indices and constituents; relies on outlier rejection and rebalance continuity for dependable reference prices. |
| Margin/settlement analyst | Uses index-value history as the audit trail behind marks and settlements. |
| Compliance/audit reviewer | Reads the audit journal of feed administration and index publication. |
| Platform administrator | Manages the service's Shield policy sets under the scoped admin role. |

## 7. KPIs / Success Criteria

- **CBBO correctness**: zero published crossed consolidated states; zero
  "best" prices sourced from empty venues.
- **Feed availability**: per-feed uptime against schedules; degraded feeds
  recovered to healthy state within the configured recovery budget (B-fill
  window + retransmission timeout).
- **Recovery quality**: gap events closed by backup-leg fill or
  retransmission rather than book reset; residual gap rate trending to zero.
- **Consolidation efficiency**: only true consolidated changes published
  (unchanged updates suppressed).
- **Index integrity**: rebalances leave the published level unchanged; outlier
  rejection removes target distortion without over-filtering.
- **Latency**: consolidated recompute fast enough that downstream routing and
  publication never observe venue lag (design budget < 50 µs per recompute).
- **Security**: no plaintext credentials anywhere; every administrative change
  auditable; no tenant can observe another tenant's feeds (RLS).
- **Operational safety**: a venue feed failing has no measurable impact on
  sibling venues' event flow.

## 8. Dependencies & Reliance on Other Services

| Dependency | Relied on for |
|---|---|
| Sentin8:Auth | Token issuance and verification, machine-client registration for service-to-service tokens |
| Shield (Sentin8:Policy) | All authorization decisions on the admin surface; policy-set hosting and hot reload |
| Sentin8:Bus | Durable publication of market events; consumption paths for consumers |
| Sentin8:Sys | Audit ingestion; hostname resolution for database-free TLS host-context |
| Sentin8:Secrets | Resolving feed credential references |
| Ex8:RefData | The security master referenced by constituents and summaries (declared read-only cross-schema access) |
| Venues' recovery endpoints | Retransmission ranges when the backup leg cannot fill a gap |
| Platform shell (sentin8::shell) | Boot order, DB pool, health, middleware, outbox relay, audit pusher, TLS termination |

## 9. Acceptance Criteria

1. Adding a new venue requires only an adapter implementation and a feed
   configuration row — no changes to any consuming service.
2. Given quotes from multiple venues, the published CBBO always equals the
   max-non-empty-bid / min-non-empty-ask with the contributing venue named.
3. A crossed consolidated book is contained by zeroing and alerting, with the
   recomputed CBBO published and the crossed pair never emitted.
4. Clearing a venue's bid, ask, or both immediately recomputes and (if
   changed) republishes the CBBO.
5. A sequence gap on the primary leg is filled by the backup leg; if the
   backup cannot fill it in time, exactly the missing range is requested from
   the venue recovery endpoint; delivered data is always in strict order.
6. An index computed with outlier rejection is insensitive to a
   deliberately distorted constituent; disabling rejection reverts to the
   plain method aggregate.
7. A constituent-set rebalance adjusts only the divisor; the published index
   level is unchanged at the change boundary.
8. Every published index value is retrievable with its timestamp.
9. All service tables are RLS-enforced: without tenant context nothing is
   visible or writable; another environment's rows are never visible.
10. A service role holder gets no bypass of row security, and every
   administrative operation is journaled for audit before its event is emitted.

## 10. Open Business Questions

- Session-supervisor wiring for the venue transports and event-emission sites
  on the engine paths are the remaining build items inside this unit (see
  `DESIGN.md` §10); until wired, feed connectivity depends on the runtime
  assembly of the pure cores.
- Publication cadence for CBBO and index values (per tick vs periodic
  snapshots) and the staleness window consumers should apply are pending a
  product decision.
- Whether additional venue adapters in the design's venue matrix (Alpha/XATS
  L1, Aequitas LIT/NEO/DARK book types) enter the first production adapter set
  is an open scope question.
- The metric vocabulary exposed to operators (cross alerts, per-feed message
  rates, gap counts, consolidation latency) needs a named dashboard home in
  the platform's observability product.

---

**Design attribution:** exchange8:vfeeds and its underlying concepts were designed by Hamid Jahanzad <Hamid@Jahanzad.com>.
