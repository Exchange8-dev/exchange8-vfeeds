// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:VFeeds BBO engine — per-instrument consolidated best bid/offer
//! with strictly non-empty venue-quote selection.
//!
//! Per-instrument `VenueQuotes[venue_index]`; CBBO bid = max **non-zero** bid,
//! ask = min **non-zero** ask. On change → `BboUpdate` event. Cross detection
//! (bid > ask across venues) → both sides zeroed + alert. Stale-quote clear
//! admin op (`POST /v1/admin/quotes/clear`).
//!
//! Design: `design/sentin8-exchange8-venue-feeds-technical-design.md` §4.3.

use exchange8::fixed::Price;

/// A single venue's quote for one instrument (raw scale 4 fixed-point).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VenueQuote {
    pub bid: Option<Price>,
    pub ask: Option<Price>,
}

impl VenueQuote {
    pub fn empty() -> Self {
        VenueQuote {
            bid: None,
            ask: None,
        }
    }
}

/// Consolidated BBO snapshot (both raw prices carry source venue indices).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bbo {
    /// Max non-zero bid across venues.
    pub bid: Option<Price>,
    /// Min non-zero ask across venues.
    pub ask: Option<Price>,
    /// Venue index contributing the best bid.
    pub bid_venue: Option<usize>,
    /// Venue index contributing the best ask.
    pub ask_venue: Option<usize>,
}

/// Admin-op outcome for stale-quote clearing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuoteUpdate {
    /// CBBO unchanged (venue quote touched but consolidated best identical).
    Unchanged,
    /// CBBO changed.
    Changed(Bbo),
    /// Cross detected (bid > ask across venues): both sides zeroed + alert.
    CrossZeroed(Bbo),
}

/// Per-instrument consolidated BBO state across venues.
///
/// Venue quotes are stored as `Option`s (`None` = no quote), so a venue with
/// no quote can never win a best-scan comparison; the consolidated sides
/// always come from venues that actually quoted.
pub struct BboEngine {
    /// Venue index → quote. Dense; index = `venue_index` from the feed
    /// configuration.
    quotes: Vec<VenueQuote>,
    /// Last computed CBBO (None = never computed).
    last: Option<Bbo>,
    /// Cross-alert count for metrics (`vfeeds_bbo_cross_alerts_total`).
    pub cross_alerts: u64,
}

impl BboEngine {
    pub fn new(venue_count: usize) -> Self {
        BboEngine {
            quotes: vec![VenueQuote::empty(); venue_count],
            last: None,
            cross_alerts: 0,
        }
    }

    /// Recompute the consolidated best from all venue quotes.
    fn compute(&self) -> Bbo {
        let mut bid: Option<(Price, usize)> = None;
        let mut ask: Option<(Price, usize)> = None;
        for (idx, q) in self.quotes.iter().enumerate() {
            if let Some(b) = q.bid {
                // Best bid = highest.
                if bid.is_none() || b.raw > bid.unwrap().0.raw {
                    bid = Some((b, idx));
                }
            }
            if let Some(a) = q.ask {
                // Best ask = lowest.
                if ask.is_none() || a.raw < ask.unwrap().0.raw {
                    ask = Some((a, idx));
                }
            }
        }
        Bbo {
            bid: bid.map(|(p, _)| p),
            ask: ask.map(|(p, _)| p),
            bid_venue: bid.map(|(_, v)| v),
            ask_venue: ask.map(|(_, v)| v),
        }
    }

    /// Zero a venue quote (stale-quote clear admin op, see DESIGN.md §4).
    fn clear_side(&mut self, venue: usize, side: ClearSide) -> Option<Bbo> {
        let q = self.quotes.get_mut(venue)?;
        match side {
            ClearSide::Bid => q.bid = None,
            ClearSide::Ask => q.ask = None,
            ClearSide::Both => *q = VenueQuote::empty(),
        }
        let now = self.compute();
        let changed = Some(now) != self.last;
        self.last = Some(now);
        changed.then_some(now)
    }

    /// A venue pushed a two-sided quote (bid/ask are raw fixed-point).
    pub fn on_quote(
        &mut self,
        venue: usize,
        bid: Option<Price>,
        ask: Option<Price>,
    ) -> QuoteUpdate {
        if venue >= self.quotes.len() {
            return QuoteUpdate::Unchanged;
        }
        self.quotes[venue] = VenueQuote { bid, ask };
        let now = self.compute();
        // Cross detection: empty venues are excluded, so both sides must be
        // present; a crossed consolidated book is a venue-desync alert.
        if let (Some(b), Some(a)) = (now.bid, now.ask) {
            if b.raw > a.raw {
                self.cross_alerts += 1;
                // Zero both sides, recompute, emit.
                self.quotes[venue] = VenueQuote::empty();
                let zeroed = self.compute();
                self.last = Some(zeroed);
                return QuoteUpdate::CrossZeroed(zeroed);
            }
        }
        let changed = Some(now) != self.last;
        self.last = Some(now);
        if changed {
            QuoteUpdate::Changed(now)
        } else {
            QuoteUpdate::Unchanged
        }
    }

    /// Clears one venue, one side, or both.
    pub fn clear_quote(&mut self, venue: usize, side: ClearSide) -> QuoteUpdate {
        match self.clear_side(venue, side) {
            Some(bbo) => QuoteUpdate::Changed(bbo),
            None => QuoteUpdate::Unchanged,
        }
    }

    /// Current CBBO (None before the first quote).
    pub fn bbo(&self) -> Option<Bbo> {
        self.last
    }
}

/// Which side(s) to clear. Part of the admin API surface (
/// `ClearQuote` accepted side: bid|ask|both) — `Ask` is reachable through
/// `POST /v1/admin/quotes/clear` even though the unit tests only cover bid/both.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClearSide {
    Bid,
    Ask,
    Both,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(raw: i128) -> Price {
        Price::equity(raw)
    }

    #[test]
    fn best_bid_max_ask_min() {
        let mut e = BboEngine::new(3);
        // venue 0: 10.00 / 10.02 ; venue 1: 10.01 / 10.03
        let r = e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        assert!(matches!(r, QuoteUpdate::Changed(_)));
        let r = e.on_quote(1, Some(p(100_100)), Some(p(100_300)));
        assert!(matches!(r, QuoteUpdate::Changed(_)));
        let bbo = e.bbo().unwrap();
        assert_eq!(bbo.bid.unwrap().raw, 100_100); // max bid
        assert_eq!(bbo.ask.unwrap().raw, 100_200); // min ask
        assert_eq!(bbo.bid_venue, Some(1));
        assert_eq!(bbo.ask_venue, Some(0));
    }

    #[test]
    fn empty_venues_excluded() {
        // With None-based storage, a venue that never quoted is
        // excluded from the best-ask scan.
        let mut e = BboEngine::new(3);
        e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        // venue 1 stays empty
        let r = e.on_quote(1, None, None);
        assert!(matches!(r, QuoteUpdate::Unchanged));
        let bbo = e.bbo().unwrap();
        assert_eq!(bbo.ask.unwrap().raw, 100_200);
        assert_eq!(bbo.ask_venue, Some(0));
    }

    #[test]
    fn unchanged_when_quote_unchanged() {
        let mut e = BboEngine::new(2);
        e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        // Re-push the identical quote → consolidated book unchanged.
        let r = e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        assert!(matches!(r, QuoteUpdate::Unchanged));
    }

    #[test]
    fn cross_detection_zeroes_and_alerts() {
        let mut e = BboEngine::new(3);
        e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        // venue 1 crosses: bid 10.03 > consolidated ask 10.02
        let r = e.on_quote(1, Some(p(100_300)), Some(p(100_400)));
        assert!(matches!(r, QuoteUpdate::CrossZeroed(_)));
        assert_eq!(e.cross_alerts, 1);
        // Both sides of the CBBO recomputed without the crossing venue.
        let bbo = e.bbo().unwrap();
        assert_eq!(bbo.bid.unwrap().raw, 100_000);
        assert_eq!(bbo.ask.unwrap().raw, 100_200);
    }

    #[test]
    fn clear_quote_bid_side() {
        let mut e = BboEngine::new(2);
        e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        e.on_quote(1, Some(p(100_100)), Some(p(100_300)));
        // Clear venue 1's bid → best bid reverts to venue 0.
        let r = e.clear_quote(1, ClearSide::Bid);
        assert!(matches!(r, QuoteUpdate::Changed(_)));
        let bbo = e.bbo().unwrap();
        assert_eq!(bbo.bid.unwrap().raw, 100_000);
        assert_eq!(bbo.bid_venue, Some(0));
    }

    #[test]
    fn clear_quote_both_sides() {
        let mut e = BboEngine::new(2);
        e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        // Clearing the only venue's both → no quotes left → CBBO empty.
        let r = e.clear_quote(0, ClearSide::Both);
        assert!(matches!(r, QuoteUpdate::Changed(_)));
        let bbo = e.bbo().unwrap();
        assert!(bbo.bid.is_none());
        assert!(bbo.ask.is_none());
    }

    #[test]
    fn clear_unknown_venue_is_noop() {
        let mut e = BboEngine::new(2);
        e.on_quote(0, Some(p(100_000)), Some(p(100_200)));
        let r = e.clear_quote(9, ClearSide::Both);
        assert!(matches!(r, QuoteUpdate::Unchanged));
    }

    #[test]
    fn one_sided_quotes_consolidate() {
        // Bid-only and ask-only venues each contribute their side.
        let mut e = BboEngine::new(2);
        e.on_quote(0, Some(p(100_000)), None);
        let r = e.on_quote(1, None, Some(p(100_300)));
        assert!(matches!(r, QuoteUpdate::Changed(_)));
        let bbo = e.bbo().unwrap();
        assert_eq!(bbo.bid.unwrap().raw, 100_000);
        assert_eq!(bbo.ask.unwrap().raw, 100_300);
    }

    #[test]
    fn out_of_range_venue_ignored() {
        let mut e = BboEngine::new(2);
        let r = e.on_quote(5, Some(p(100_000)), Some(p(100_200)));
        assert!(matches!(r, QuoteUpdate::Unchanged));
        assert!(e.bbo().is_none());
    }

    #[test]
    fn wide_cross_between_venues_zeroes_all() {
        // venue 0 posts 10.50 bid, venue 1 posts 10.00 ask → cross.
        let mut e = BboEngine::new(2);
        e.on_quote(0, Some(p(100_500)), None);
        let r = e.on_quote(1, None, Some(p(100_000)));
        assert!(matches!(r, QuoteUpdate::CrossZeroed(_)));
        assert_eq!(e.cross_alerts, 1);
        let bbo = e.bbo().unwrap();
        // crossing venue removed → both sides from remaining venue-0
        // (bid-only) → CBBO bid 10.50, ask None.
        assert_eq!(bbo.bid.unwrap().raw, 100_500);
        assert!(bbo.ask.is_none());
    }
}
