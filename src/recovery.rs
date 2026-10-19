// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:VFeeds dual-feed A/B sequence gap recovery.
//!
//! Implements venue-feeds design §4.1: dual-feed (A/B) consumption with
//! sequence-gap recovery. A is the primary stream; B is the backup. When A
//! gaps, the consumer waits for the missing sequence on B (bounded wait),
//! then requests retransmission from the venue recovery endpoint for
//! anything still missing.
//!
//! Sequence rules:
//! - Each feed advances its own monotonically increasing sequence.
//! - A message is *new* when its seq == consumer's next expected; *stale*
//!   when below (dedup); *gap* when above (missing predecessors).
//! - Gap policy: buffer out-of-order messages (bounded window), wait for
//!   B to fill, then emit a retransmission request for the remaining hole.
//!
//! Pure core; the shell owns the sockets/timers and drives `on_message` /
//! `poll_retransmit`.
//!
//! Design: `design/sentin8-exchange8-venue-feeds-technical-design.md` §4.1.

use std::collections::BTreeMap;

/// Which physical feed a message arrived on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedId {
    A,
    B,
}

/// Feed health as observed by the sequencer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeedHealth {
    /// Receiving in-order messages.
    Healthy,
    /// A gap is open; waiting on B / retransmission.
    Recovering,
}

/// One inbound venue message (transport-agnostic).
#[derive(Clone, Debug)]
pub struct FeedMessage {
    pub feed: FeedId,
    pub seq: u64,
    /// Payload bytes (parsed by the adapter downstream).
    pub payload: Vec<u8>,
}

/// A retransmission request the shell sends to the venue recovery endpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetransmitRequest {
    /// First missing sequence (inclusive).
    pub from_seq: u64,
    /// Last missing sequence (inclusive).
    pub to_seq: u64,
    /// Count of messages requested.
    pub count: u64,
}

/// The A/B sequencer for one venue feed session.
pub struct AbSequencer {
    /// Next sequence the consumer is ready to deliver.
    next_seq: u64,
    /// Buffered out-of-order messages by seq (from either feed).
    buffer: BTreeMap<u64, FeedMessage>,
    /// Highest sequence seen per feed.
    last_seen: [u64; 2],
    /// Open retransmission requests (not yet satisfied).
    pending_request: Option<RetransmitRequest>,
    /// Messages delivered in order (ready for the adapter).
    ready: Vec<FeedMessage>,
    /// Max buffered messages before forced retransmission (backstop).
    buffer_cap: usize,
}

impl AbSequencer {
    /// New sequencer starting at `start_seq` (typically the session's
    /// first sequence, e.g. 1 or the login heartbeat value).
    pub fn new(start_seq: u64, buffer_cap: usize) -> Self {
        AbSequencer {
            next_seq: start_seq,
            buffer: BTreeMap::new(),
            last_seen: [0; 2],
            pending_request: None,
            ready: Vec::new(),
            buffer_cap,
        }
    }

    /// Ingest one message. Returns the feed's health after ingestion.
    pub fn on_message(&mut self, msg: FeedMessage) -> FeedHealth {
        let fi = match msg.feed {
            FeedId::A => 0,
            FeedId::B => 1,
        };
        if msg.seq > self.last_seen[fi] {
            self.last_seen[fi] = msg.seq;
        }
        // Stale (already delivered) - drop.
        if msg.seq < self.next_seq {
            return self.health();
        }
        // Gap detected on this feed.
        if msg.seq > self.next_seq {
            self.buffer.insert(msg.seq, msg);
            if self.buffer.len() > self.buffer_cap {
                self.open_request();
            }
            return FeedHealth::Recovering;
        }
        // In-order on the nose: deliver, then drain the buffer.
        self.ready.push(msg);
        self.next_seq += 1;
        while let Some((&seq, _)) = self.buffer.first_key_value() {
            if seq == self.next_seq {
                let m = self.buffer.remove(&seq).unwrap();
                self.ready.push(m);
                self.next_seq += 1;
            } else {
                break;
            }
        }
        // Request satisfied?
        if let Some(r) = &self.pending_request {
            if r.to_seq < self.next_seq {
                self.pending_request = None;
            }
        }
        self.health()
    }

    /// B-fill window expired: whatever is still missing becomes a
    /// retransmission request to the venue.
    pub fn on_b_wait_timeout(&mut self) -> Option<RetransmitRequest> {
        self.open_request()
    }

    /// Open (or re-open) a retransmission request covering the open hole.
    fn open_request(&mut self) -> Option<RetransmitRequest> {
        // The hole is [next_seq, first_buffered - 1] when buffered messages
        // exist beyond it; otherwise we do not know the far end - use the
        // highest seen sequence across feeds.
        let to = self
            .buffer
            .keys()
            .next()
            .copied()
            .map(|k| k - 1)
            .unwrap_or_else(|| self.last_seen[0].max(self.last_seen[1]).max(self.next_seq));
        if to < self.next_seq {
            return None; // no hole
        }
        let req = RetransmitRequest {
            from_seq: self.next_seq,
            to_seq: to,
            count: to - self.next_seq + 1,
        };
        self.pending_request = Some(req);
        Some(req)
    }

    /// Drain messages now in order (the adapter consumes these).
    pub fn take_ready(&mut self) -> Vec<FeedMessage> {
        std::mem::take(&mut self.ready)
    }

    /// Current health.
    pub fn health(&self) -> FeedHealth {
        if self.pending_request.is_some() || !self.buffer.is_empty() {
            FeedHealth::Recovering
        } else {
            FeedHealth::Healthy
        }
    }

    /// Next expected sequence (diagnostics).
    pub fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Buffered count (diagnostics).
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(feed: FeedId, seq: u64) -> FeedMessage {
        FeedMessage {
            feed,
            seq,
            payload: vec![seq as u8],
        }
    }

    #[test]
    fn in_order_a_stream_delivers() {
        let mut s = AbSequencer::new(1, 64);
        assert_eq!(s.on_message(msg(FeedId::A, 1)), FeedHealth::Healthy);
        assert_eq!(s.on_message(msg(FeedId::A, 2)), FeedHealth::Healthy);
        let ready = s.take_ready();
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].seq, 1);
        assert_eq!(ready[1].seq, 2);
        assert_eq!(s.next_seq(), 3);
    }

    #[test]
    fn duplicate_and_stale_dropped() {
        let mut s = AbSequencer::new(1, 64);
        s.on_message(msg(FeedId::A, 1));
        s.take_ready();
        // Duplicate of 1 (B replay) - stale, dropped, no delivery.
        s.on_message(msg(FeedId::B, 1));
        assert!(s.take_ready().is_empty());
        // Also below next after gap fill.
        s.on_message(msg(FeedId::A, 2));
        s.take_ready();
        s.on_message(msg(FeedId::B, 1));
        assert!(s.take_ready().is_empty());
        assert_eq!(s.next_seq(), 3);
    }

    #[test]
    fn b_fills_a_gap_without_retransmission() {
        let mut s = AbSequencer::new(1, 64);
        s.on_message(msg(FeedId::A, 1));
        s.take_ready();
        // A jumps to 4 → hole 2..3, message 4 buffered.
        assert_eq!(s.on_message(msg(FeedId::A, 4)), FeedHealth::Recovering);
        // B delivers 2 and 3 → hole closes, 2,3,4 delivered in order.
        s.on_message(msg(FeedId::B, 2));
        s.on_message(msg(FeedId::B, 3));
        let ready = s.take_ready();
        assert_eq!(
            ready.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        assert_eq!(s.health(), FeedHealth::Healthy);
        assert!(s.pending_request.is_none());
    }

    #[test]
    fn b_wait_timeout_requests_retransmission() {
        let mut s = AbSequencer::new(1, 64);
        s.on_message(msg(FeedId::A, 1));
        s.take_ready();
        // A gaps: delivers 5 (hole 2..4).
        s.on_message(msg(FeedId::A, 5));
        // B wait expires without filling.
        let req = s.on_b_wait_timeout().unwrap();
        assert_eq!(req.from_seq, 2);
        assert_eq!(req.to_seq, 4);
        assert_eq!(req.count, 3);
        assert_eq!(s.health(), FeedHealth::Recovering);
    }

    #[test]
    fn retransmitted_messages_close_request() {
        let mut s = AbSequencer::new(1, 64);
        s.on_message(msg(FeedId::A, 1));
        s.take_ready();
        s.on_message(msg(FeedId::A, 5));
        let req = s.on_b_wait_timeout().unwrap();
        // Venue retransmits 2..4 (recovery endpoint, appears on A).
        for seq in req.from_seq..=req.to_seq {
            s.on_message(msg(FeedId::A, seq));
        }
        let ready = s.take_ready();
        assert_eq!(
            ready.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![2, 3, 4, 5]
        );
        assert_eq!(s.health(), FeedHealth::Healthy);
        assert!(s.pending_request.is_none());
    }

    #[test]
    fn buffer_cap_forces_early_request() {
        let mut s = AbSequencer::new(1, 4);
        s.on_message(msg(FeedId::A, 1));
        s.take_ready();
        // Jump far ahead - buffering 5 messages exceeds the cap of 4.
        s.on_message(msg(FeedId::A, 10));
        s.on_message(msg(FeedId::A, 11));
        s.on_message(msg(FeedId::A, 12));
        let before = s.buffered();
        assert!(before <= 4);
        // Either the cap triggered a request or buffering continues within
        // the cap; after the 5th buffered message a request must exist.
        s.on_message(msg(FeedId::A, 13));
        s.on_message(msg(FeedId::A, 14));
        assert!(s.pending_request.is_some());
    }

    #[test]
    fn out_of_order_then_flush_preserves_order() {
        let mut s = AbSequencer::new(1, 64);
        // Arrive out of order: 3, 2, then 1.
        s.on_message(msg(FeedId::A, 3));
        s.on_message(msg(FeedId::A, 2));
        s.on_message(msg(FeedId::A, 1));
        let ready = s.take_ready();
        assert_eq!(
            ready.iter().map(|m| m.seq).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(s.next_seq(), 4);
    }
}
