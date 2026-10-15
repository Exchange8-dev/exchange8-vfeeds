// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:VFeeds adapter framework + remaining venue adapters + crypto
//! REST/WS feeds + IV surface + index feed.
//!
//! - **Adapter framework**: the `VenueAdapter` trait, transport enum,
//!   payload kinds (see DESIGN.md §4) — the compile-time adapter registry.
//! - **`itch_orders`** (Chi-X ITCH-over-Mold/Soup): normalized depth
//!   events from ITCH add/update/exec messages.
//! - **`exmt_quantum`** (TSX/TSXV/TMX Select XMT): binary L1 packets
//!   → normalized quotes.
//! - **`aequitas` UDP2IP + `tl1_text` CSE**: text/TL1 quote lines.
//! - **Crypto feeds**: REST snapshot + WS delta normalized events.
//! - **IV surface**: per-instrument smile snapshot for Greeks pricing.
//! - **Index feed**: constituent ticks → index value events (feeds
//!   margin/funding).
//!
//! All adapters are pure parsers/classifiers: bytes/lines in →
//! normalized `VenueQuoteEvent` out. The shell owns sockets, A/B recovery
//! (recovery.rs), and the per-feed task supervision.
//!
//! Design: `design/sentin8-exchange8-venue-feeds-technical-design.md` §4,
//! `design/sentin8-exchange8-crypto-extension-technical-design.md`,
//! `design/sentin8-exchange8-options-technical-design.md`,
//! `design/sentin8-exchange8-futures-technical-design.md`.

// ---------------------------------------------------------------------------
// Adapter framework (see DESIGN.md §4)
// ---------------------------------------------------------------------------

/// Transport configuration (design §4.1 enum).
#[derive(Clone, Debug, PartialEq)]
pub enum Transport {
    /// Dual multicast A/B legs (recovery.rs sequencer).
    MulticastAb {
        a: String,
        b: String,
        iface: String,
        ttl: u32,
    },
    /// Single multicast.
    Multicast { group: String, iface: String },
    /// SoupBinTCP (login + sequenced + heartbeat).
    SoupTcp { host: String, port: u16 },
    /// Plain TCP (TL1/text).
    Tcp { host: String, port: u16 },
    /// Aequitas UDP2IP framing.
    Udp2Ip { host: String, port: u16 },
    /// Crypto REST snapshot poller.
    Rest { url: String, poll_ms: u64 },
    /// Crypto WebSocket delta stream.
    WebSocket { url: String },
}

/// Payload kinds the adapters normalize.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PayloadKind {
    StampCbbo,
    ItchOrders,
    ItchL1,
    ExmtQuantum,
    Udp2IpNeo,
    Tl1Text,
    CryptoRest,
    CryptoWs,
    IvSurface,
    Index,
}

/// Normalized quote event (design §4.2 shape).
#[derive(Clone, Debug, PartialEq)]
pub struct VenueQuoteEvent {
    pub env_id: String,
    pub instrument_id: u32,
    pub venue_mic: [u8; 4],
    pub ts_venue: u64,
    pub ts_recv: u64,
    pub kind: QuoteKind,
    /// Bid (price raw, size).
    pub bid: Option<(i128, u64)>,
    /// Ask (price raw, size).
    pub ask: Option<(i128, u64)>,
    /// Depth levels (kind = Depth).
    pub depth: Option<Vec<(i128, u64, bool)>>, // (price, qty, is_bid)
    pub lot_size: Option<u32>,
    /// Venue sequence when available.
    pub seq: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuoteKind {
    Best,
    Depth,
    OrderBook,
    Status,
}

/// The adapter contract (design §4.1 trait, sync-pure form).
pub trait VenueAdapter: Send + Sync {
    /// Adapter registry key (compile-time known set).
    fn name(&self) -> &'static str;
    /// Kinds this adapter produces.
    fn supported(&self) -> &'static [PayloadKind];
    /// Transport this instance is configured with.
    fn transport(&self) -> &Transport;
    /// Parse one transport frame into normalized events.
    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent>;
}

/// Feed config row (projection of `vfeeds.external_feed`).
#[derive(Clone, Debug)]
pub struct FeedConfig {
    pub feed_id: u64,
    pub adapter: String,
    pub transport: Transport,
    /// Login credential secret reference (never plaintext).
    pub creds_secret_ref: Option<String>,
}

// ---------------------------------------------------------------------------
// itch_orders (Chi-X ITCH-over-Mold/Soup) depth normalization
// ---------------------------------------------------------------------------

/// Chi-X ITCH message headers we normalize (text-protocol subset).
pub struct ItchOrdersAdapter {
    pub transport: Transport,
}

impl VenueAdapter for ItchOrdersAdapter {
    fn name(&self) -> &'static str {
        "itch_orders"
    }

    fn supported(&self) -> &'static [PayloadKind] {
        &[PayloadKind::ItchOrders, PayloadKind::ItchL1]
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent> {
        // Line-oriented ITCH-text: 'A' add, 'E' execute, 'X' cancel,
        // 'Q' cross/bbo. Pipe-separated fields.
        let text = String::from_utf8_lossy(frame);
        let mut out = Vec::new();
        for line in text.lines() {
            let f: Vec<&str> = line.split('|').collect();
            match f.first().copied() {
                Some("Q") if f.len() >= 6 => {
                    // Q|inst|bidpx|bidsize|askpx|asksize
                    let inst: u32 = f[1].parse().unwrap_or(0);
                    let bid = f[2].parse::<i128>().ok().filter(|p| *p > 0);
                    let ask = f[4].parse::<i128>().ok().filter(|p| *p > 0);
                    out.push(VenueQuoteEvent {
                        env_id: String::new(),
                        instrument_id: inst,
                        venue_mic: *b"CHIC",
                        ts_venue: 0,
                        ts_recv: 0,
                        kind: QuoteKind::Best,
                        bid: bid.map(|p| (p, f[3].parse().unwrap_or(0))),
                        ask: ask.map(|p| (p, f[5].parse().unwrap_or(0))),
                        depth: None,
                        lot_size: None,
                        seq: 0,
                    });
                }
                Some("A") if f.len() >= 6 => {
                    // A|inst|side(B/S)|px|qty|seq → depth update.
                    let inst: u32 = f[1].parse().unwrap_or(0);
                    let is_bid = f[2] == "B";
                    let px: i128 = f[3].parse().unwrap_or(0);
                    let qty: u64 = f[4].parse().unwrap_or(0);
                    out.push(VenueQuoteEvent {
                        env_id: String::new(),
                        instrument_id: inst,
                        venue_mic: *b"CHIC",
                        ts_venue: 0,
                        ts_recv: 0,
                        kind: QuoteKind::Depth,
                        bid: None,
                        ask: None,
                        depth: Some(vec![(px, qty, is_bid)]),
                        lot_size: None,
                        seq: f[5].parse().unwrap_or(0),
                    });
                }
                _ => {}
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// exmt_quantum (TSX XMT) binary L1
// ---------------------------------------------------------------------------

/// XMT Quantum binary L1 packet: [type:u8][inst:u32][bid:u32][ask:u32]
/// little-endian, prices ×10⁴.
pub struct ExmtQuantumAdapter {
    pub transport: Transport,
}

impl VenueAdapter for ExmtQuantumAdapter {
    fn name(&self) -> &'static str {
        "exmt_quantum"
    }

    fn supported(&self) -> &'static [PayloadKind] {
        &[PayloadKind::ExmtQuantum]
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent> {
        let mut out = Vec::new();
        let mut off = 0;
        while off + 13 <= frame.len() {
            let ty = frame[off];
            if ty != b'L' {
                off += 13;
                continue;
            }
            let inst = u32::from_le_bytes([
                frame[off + 1],
                frame[off + 2],
                frame[off + 3],
                frame[off + 4],
            ]);
            let bid = u32::from_le_bytes([
                frame[off + 5],
                frame[off + 6],
                frame[off + 7],
                frame[off + 8],
            ]);
            let ask = u32::from_le_bytes([
                frame[off + 9],
                frame[off + 10],
                frame[off + 11],
                frame[off + 12],
            ]);
            out.push(VenueQuoteEvent {
                env_id: String::new(),
                instrument_id: inst,
                venue_mic: *b"XTSE",
                ts_venue: 0,
                ts_recv: 0,
                kind: QuoteKind::Best,
                bid: (bid > 0).then_some((bid as i128, 0)),
                ask: (ask > 0).then_some((ask as i128, 0)),
                depth: None,
                lot_size: None,
                seq: 0,
            });
            off += 13;
        }
        out
    }
}

// ---------------------------------------------------------------------------
// aequitas UDP2IP + tl1_text
// ---------------------------------------------------------------------------

/// Aequitas UDP2IP: framed quotes `[len:u16][inst:u32][bid:u32][ask:u32]`.
pub struct AequitasAdapter {
    pub transport: Transport,
}

impl VenueAdapter for AequitasAdapter {
    fn name(&self) -> &'static str {
        "aequitas"
    }

    fn supported(&self) -> &'static [PayloadKind] {
        &[PayloadKind::Udp2IpNeo]
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent> {
        let mut out = Vec::new();
        let mut off = 0;
        while off + 2 <= frame.len() {
            let len = u16::from_le_bytes([frame[off], frame[off + 1]]) as usize;
            if len < 12 || off + 2 + len > frame.len() {
                break;
            }
            let body = &frame[off + 2..off + 2 + len];
            let inst = u32::from_le_bytes([body[0], body[1], body[2], body[3]]);
            let bid = u32::from_le_bytes([body[4], body[5], body[6], body[7]]);
            let ask = u32::from_le_bytes([body[8], body[9], body[10], body[11]]);
            out.push(VenueQuoteEvent {
                env_id: String::new(),
                instrument_id: inst,
                venue_mic: *b"AEQA",
                ts_venue: 0,
                ts_recv: 0,
                kind: QuoteKind::Best,
                bid: (bid > 0).then_some((bid as i128, 0)),
                ask: (ask > 0).then_some((ask as i128, 0)),
                depth: None,
                lot_size: None,
                seq: 0,
            });
            off += 2 + len;
        }
        out
    }
}

/// CSE TL1 text: `QUOTE inst bid ask` lines.
pub struct Tl1TextAdapter {
    pub transport: Transport,
}

impl VenueAdapter for Tl1TextAdapter {
    fn name(&self) -> &'static str {
        "tl1_text"
    }

    fn supported(&self) -> &'static [PayloadKind] {
        &[PayloadKind::Tl1Text]
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent> {
        let text = String::from_utf8_lossy(frame);
        let mut out = Vec::new();
        for line in text.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() == 4 && f[0] == "QUOTE" {
                let inst: u32 = f[1].parse().unwrap_or(0);
                let bid: i128 = f[2].parse().unwrap_or(0);
                let ask: i128 = f[3].parse().unwrap_or(0);
                out.push(VenueQuoteEvent {
                    env_id: String::new(),
                    instrument_id: inst,
                    venue_mic: *b"XCNQ",
                    ts_venue: 0,
                    ts_recv: 0,
                    kind: QuoteKind::Best,
                    bid: (bid > 0).then_some((bid, 0)),
                    ask: (ask > 0).then_some((ask, 0)),
                    depth: None,
                    lot_size: None,
                    seq: 0,
                });
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Crypto REST snapshot + WS delta
// ---------------------------------------------------------------------------

/// Crypto REST snapshot JSON: {"i":1,"bid_px":99.5,"bid_qty":3.5,...}
/// (prices ×10⁸ raw from the exchange's scaled decimals).
pub struct CryptoRestAdapter {
    pub transport: Transport,
}

impl VenueAdapter for CryptoRestAdapter {
    fn name(&self) -> &'static str {
        "crypto_rest"
    }

    fn supported(&self) -> &'static [PayloadKind] {
        &[PayloadKind::CryptoRest]
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent> {
        let mut out = Vec::new();
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(frame) {
            let inst = v.get("i").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
            let get_pair = |k: &str| -> Option<(i128, u64)> {
                let px = v.get(format!("{k}_px")).and_then(|x| x.as_i64())?;
                let qty = v
                    .get(format!("{k}_qty"))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0);
                Some((px as i128, qty))
            };
            out.push(VenueQuoteEvent {
                env_id: String::new(),
                instrument_id: inst,
                venue_mic: *b"XL8C",
                ts_venue: 0,
                ts_recv: 0,
                kind: QuoteKind::Best,
                bid: get_pair("bid"),
                ask: get_pair("ask"),
                depth: None,
                lot_size: Some(1),
                seq: 0,
            });
        }
        out
    }
}

/// Crypto WS delta: {"e":"depth","i":1,"s":"bid","p":..,"q":..} one level.
pub struct CryptoWsAdapter {
    pub transport: Transport,
}

impl VenueAdapter for CryptoWsAdapter {
    fn name(&self) -> &'static str {
        "crypto_ws"
    }

    fn supported(&self) -> &'static [PayloadKind] {
        &[PayloadKind::CryptoWs]
    }

    fn transport(&self) -> &Transport {
        &self.transport
    }

    fn parse(&self, frame: &[u8]) -> Vec<VenueQuoteEvent> {
        let mut out = Vec::new();
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(frame) {
            if v.get("e").and_then(|x| x.as_str()) == Some("depth") {
                let inst = v.get("i").and_then(|x| x.as_u64()).unwrap_or(0) as u32;
                let side_bid = v.get("s").and_then(|x| x.as_str()) == Some("bid");
                let px = v.get("p").and_then(|x| x.as_i64()).unwrap_or(0);
                let qty = v.get("q").and_then(|x| x.as_u64()).unwrap_or(0);
                out.push(VenueQuoteEvent {
                    env_id: String::new(),
                    instrument_id: inst,
                    venue_mic: *b"XL8C",
                    ts_venue: 0,
                    ts_recv: 0,
                    kind: QuoteKind::Depth,
                    bid: None,
                    ask: None,
                    depth: Some(vec![(px as i128, qty, side_bid)]),
                    lot_size: Some(1),
                    seq: 0,
                });
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// IV surface + index feed
// ---------------------------------------------------------------------------

/// IV surface point: (strike, expiry_days, iv_bps).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IvPoint {
    pub strike_raw: i128,
    pub expiry_days: u32,
    /// Implied vol in bps (10_000 = 100%).
    pub iv_bps: u32,
}

/// Per-instrument IV surface snapshot (Greeks pricing input).
#[derive(Clone, Debug, PartialEq, Default)]
pub struct IvSurface {
    pub instrument_id: u32,
    pub points: Vec<IvPoint>,
}

impl IvSurface {
    /// Interpolated IV at (strike, expiry): nearest-expiry band, linear in
    /// strike between bracketing points; flat outside the range.
    pub fn iv_at(&self, strike_raw: i128, expiry_days: u32) -> Option<u32> {
        if self.points.is_empty() {
            return None;
        }
        // Nearest expiry band.
        let band: Vec<&IvPoint> = {
            let mut sorted: Vec<&IvPoint> = self.points.iter().collect();
            sorted.sort_by_key(|p| (p.expiry_days, p.strike_raw));
            let nearest = sorted
                .iter()
                .map(|p| p.expiry_days)
                .min_by_key(|d| d.abs_diff(expiry_days))?;
            sorted
                .into_iter()
                .filter(|p| p.expiry_days == nearest)
                .collect()
        };
        // Bracket by strike.
        let below = band.iter().rfind(|p| p.strike_raw <= strike_raw);
        let above = band.iter().find(|p| p.strike_raw >= strike_raw);
        match (below, above) {
            (Some(b), Some(a)) => {
                if a.strike_raw == b.strike_raw {
                    Some(a.iv_bps)
                } else {
                    // Linear interpolation (f64 end-to-end; a negative
                    // delta cast `as u32` would saturate at 0, so round
                    // the final value, not the delta).
                    let span = (a.strike_raw - b.strike_raw) as f64;
                    let t = (strike_raw - b.strike_raw) as f64 / span;
                    let v = b.iv_bps as f64 + (a.iv_bps as f64 - b.iv_bps as f64) * t;
                    Some(v.round() as u32)
                }
            }
            (Some(b), None) => Some(b.iv_bps),
            (None, Some(a)) => Some(a.iv_bps),
            (None, None) => None,
        }
    }
}

/// Index constituent tick (index feed input; see DESIGN.md §4).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct IndexTick {
    pub constituent_id: u32,
    /// Last price (raw).
    pub price_raw: i128,
}

/// Index value computation (price-weighted / cap-weighted by weight sum).
#[derive(Clone, Debug, PartialEq)]
pub struct IndexValue {
    pub index_id: u32,
    /// Weighted sum of prices.
    pub value_raw: i128,
    /// Divisor applied.
    pub divisor: i128,
}

/// Compute an index value from constituent ticks (weights = share counts).
pub fn compute_index(
    index_id: u32,
    ticks: &[IndexTick],
    weights: &std::collections::HashMap<u32, u64>,
    divisor: i128,
) -> Option<IndexValue> {
    if divisor == 0 || ticks.is_empty() {
        return None;
    }
    let sum: i128 = ticks
        .iter()
        .map(|t| {
            let w = weights.get(&t.constituent_id).copied().unwrap_or(0) as i128;
            t.price_raw.saturating_mul(w)
        })
        .sum();
    Some(IndexValue {
        index_id,
        value_raw: sum / divisor,
        divisor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tcp() -> Transport {
        Transport::Tcp {
            host: "127.0.0.1".into(),
            port: 9000,
        }
    }

    #[test]
    fn itch_orders_parses_quotes_and_depth() {
        let a = ItchOrdersAdapter { transport: tcp() };
        let frame = b"Q|7|99000|100|101000|200\nA|7|B|98900|300|55\n";
        let ev = a.parse(frame);
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].kind, QuoteKind::Best);
        assert_eq!(ev[0].bid, Some((99_000, 100)));
        assert_eq!(ev[0].ask, Some((101_000, 200)));
        assert_eq!(ev[1].kind, QuoteKind::Depth);
        assert_eq!(ev[1].depth.as_ref().unwrap()[0], (98_900, 300, true));
        assert_eq!(ev[1].venue_mic, *b"CHIC");
        // Adapter metadata.
        assert_eq!(a.name(), "itch_orders");
        assert!(a.supported().contains(&PayloadKind::ItchOrders));
    }

    #[test]
    fn itch_ignores_unknown_lines() {
        let a = ItchOrdersAdapter { transport: tcp() };
        assert!(a.parse(b"S|1|2\nX|short\n").is_empty());
    }

    #[test]
    fn exmt_quantum_parses_binary_l1() {
        let a = ExmtQuantumAdapter { transport: tcp() };
        let mut frame = vec![b'L'];
        frame.extend_from_slice(&7u32.to_le_bytes());
        frame.extend_from_slice(&99_000u32.to_le_bytes());
        frame.extend_from_slice(&101_000u32.to_le_bytes());
        // A non-L record skipped.
        frame.extend_from_slice(&[0u8; 13]);
        let ev = a.parse(&frame);
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].instrument_id, 7);
        assert_eq!(ev[0].bid, Some((99_000, 0)));
        assert_eq!(ev[0].ask, Some((101_000, 0)));
        assert_eq!(ev[0].venue_mic, *b"XTSE");
    }

    #[test]
    fn aequitas_parses_framed_packets() {
        let a = AequitasAdapter { transport: tcp() };
        let mut frame = Vec::new();
        let body_len = 12u16;
        frame.extend_from_slice(&body_len.to_le_bytes());
        frame.extend_from_slice(&5u32.to_le_bytes());
        frame.extend_from_slice(&50_000u32.to_le_bytes());
        frame.extend_from_slice(&51_000u32.to_le_bytes());
        let ev = a.parse(&frame);
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].instrument_id, 5);
        assert_eq!(ev[0].bid, Some((50_000, 0)));
        assert_eq!(ev[0].venue_mic, *b"AEQA");
    }

    #[test]
    fn aequitas_stops_on_truncated() {
        let a = AequitasAdapter { transport: tcp() };
        // Claims 12-byte body but only 4 present.
        let frame = [12u8, 0, 1, 2, 3, 4];
        assert!(a.parse(&frame).is_empty());
    }

    #[test]
    fn tl1_parses_quote_lines() {
        let a = Tl1TextAdapter { transport: tcp() };
        let ev = a.parse(b"QUOTE 9 99000 101000\nIGNORE this\nQUOTE 10 0 0\n");
        // Two events: the IGNORE line is skipped; the zero-quote line still
        // emits an event with both sides None.
        assert_eq!(ev.len(), 2);
        assert_eq!(ev[0].instrument_id, 9);
        assert_eq!(ev[0].bid, Some((99_000, 0)));
        assert_eq!(ev[1].instrument_id, 10);
        assert_eq!(ev[1].bid, None);
        assert_eq!(ev[1].ask, None);
        assert_eq!(ev[0].venue_mic, *b"XCNQ");
    }

    #[test]
    fn crypto_rest_snapshot_json() {
        let a = CryptoRestAdapter {
            transport: Transport::Rest {
                url: "https://x".into(),
                poll_ms: 500,
            },
        };
        let frame = br#"{"i":1,"bid_px":950000000,"bid_qty":350000000,"ask_px":951000000,"ask_qty":120000000}"#;
        let ev = a.parse(frame);
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].bid, Some((950_000_000, 350_000_000)));
        assert_eq!(ev[0].ask, Some((951_000_000, 120_000_000)));
        assert_eq!(ev[0].lot_size, Some(1));
    }

    #[test]
    fn crypto_ws_depth_delta() {
        let a = CryptoWsAdapter {
            transport: Transport::WebSocket {
                url: "wss://x".into(),
            },
        };
        let frame = br#"{"e":"depth","i":1,"s":"bid","p":950000000,"q":100}"#;
        let ev = a.parse(frame);
        assert_eq!(ev.len(), 1);
        assert_eq!(ev[0].kind, QuoteKind::Depth);
        assert_eq!(ev[0].depth.as_ref().unwrap()[0], (950_000_000, 100, true));
        // Non-depth events ignored.
        assert!(a.parse(br#"{"e":"trade","i":1}"#).is_empty());
    }

    #[test]
    fn iv_surface_interpolates() {
        let s = IvSurface {
            instrument_id: 1,
            points: vec![
                IvPoint {
                    strike_raw: 9_000,
                    expiry_days: 30,
                    iv_bps: 2_000,
                },
                IvPoint {
                    strike_raw: 10_000,
                    expiry_days: 30,
                    iv_bps: 1_800,
                },
                IvPoint {
                    strike_raw: 11_000,
                    expiry_days: 30,
                    iv_bps: 1_900,
                },
                IvPoint {
                    strike_raw: 10_000,
                    expiry_days: 90,
                    iv_bps: 2_100,
                },
            ],
        };
        // Exact node.
        assert_eq!(s.iv_at(10_000, 30), Some(1_800));
        // Between nodes: 9_500 → 1_900.
        assert_eq!(s.iv_at(9_500, 30), Some(1_900));
        // Outside range: flat at the wing.
        assert_eq!(s.iv_at(8_000, 30), Some(2_000));
        assert_eq!(s.iv_at(12_000, 30), Some(1_900));
        // Nearest expiry band (90 vs requested 80 → 90 band).
        assert_eq!(s.iv_at(10_000, 80), Some(2_100));
        // Empty surface.
        assert_eq!(IvSurface::default().iv_at(1, 1), None);
    }

    #[test]
    fn index_value_computes() {
        use std::collections::HashMap;
        let mut weights = HashMap::new();
        weights.insert(1u32, 2u64);
        weights.insert(2u32, 3u64);
        let ticks = vec![
            IndexTick {
                constituent_id: 1,
                price_raw: 10_000,
            },
            IndexTick {
                constituent_id: 2,
                price_raw: 20_000,
            },
        ];
        // Sum = 2×10_000 + 3×20_000 = 80_000; divisor 4 → 20_000.
        let idx = compute_index(9, &ticks, &weights, 4).unwrap();
        assert_eq!(idx.value_raw, 20_000);
        // Unknown constituent contributes 0.
        let ticks2 = vec![IndexTick {
            constituent_id: 99,
            price_raw: 5_000,
        }];
        let idx2 = compute_index(9, &ticks2, &weights, 4).unwrap();
        assert_eq!(idx2.value_raw, 0);
        // Zero divisor / empty ticks → None.
        assert!(compute_index(9, &ticks, &weights, 0).is_none());
        assert!(compute_index(9, &[], &weights, 4).is_none());
    }

    #[test]
    fn feed_config_shapes() {
        let cfg = FeedConfig {
            feed_id: 1,
            adapter: "itch_orders".into(),
            transport: tcp(),
            creds_secret_ref: Some("secret://exchange8/chix/login".into()),
        };
        assert_eq!(cfg.adapter, "itch_orders");
        assert!(cfg
            .creds_secret_ref
            .as_deref()
            .unwrap()
            .starts_with("secret://"));
    }
}
