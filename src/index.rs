// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
// SPDX-License-Identifier: Apache-2.0

//! Ex8:VFeeds index engine — constituent aggregation + divisor continuity.
//!
//! Methods: median / volume-weighted / time-weighted, with outlier rejection
//! (drop constituents deviating > N% from the median). The divisor maintains
//! continuity across rebalancing.
//!
//! Design: `design/sentin8-exchange8-index-engine-technical-design.md` §2.

/// Index calculation method.
///
/// The variant set is the `vfeeds.index.method` CHECK vocabulary
/// (`services/vfeeds/sql/init.sql`); [`IndexMethod::from_db`] is the only
/// DB-string → variant path and never defaults an unknown name (callers
/// surface the error).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexMethod {
    /// Median of constituent prices (robust to outliers by construction).
    /// Schema vocabulary: `median` (the column default).
    Median,
    /// Volume-weighted price (Σ price×vol / Σ vol) over the update window.
    /// Schema vocabulary: `volume_weighted`.
    VolumeWeighted,
    /// Time-weighted: last price of each constituent, simple-weighted.
    /// Schema vocabulary: `time_weighted`.
    TimeWeighted,
}

impl IndexMethod {
    /// Decode a `vfeeds.index.method` value; `None` for a name outside the
    /// schema CHECK vocabulary (never defaults).
    ///
    /// # Examples
    /// ```text
    /// IndexMethod::from_db("median")            → Some(IndexMethod::Median)
    /// IndexMethod::from_db("volume_weighted")   → Some(IndexMethod::VolumeWeighted)
    /// IndexMethod::from_db("nonsense")          → None
    /// ```
    pub fn from_db(name: &str) -> Option<Self> {
        match name {
            "median" => Some(Self::Median),
            "volume_weighted" => Some(Self::VolumeWeighted),
            "time_weighted" => Some(Self::TimeWeighted),
            _ => None,
        }
    }

    /// Encode as the `vfeeds.index.method` schema vocabulary.
    ///
    /// # Examples
    /// ```text
    /// IndexMethod::TimeWeighted.as_str() == "time_weighted"
    /// ```
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Median => "median",
            Self::VolumeWeighted => "volume_weighted",
            Self::TimeWeighted => "time_weighted",
        }
    }
}

/// One constituent's market state at calculation time.
#[derive(Clone, Copy, Debug)]
pub struct ConstituentTick {
    pub security_id: u64,
    /// Current or VWAP price (raw ticks).
    pub price: i128,
    /// Volume in the window (0 when unavailable → weight 1 for median/time).
    pub volume: i128,
    /// Configured weight (from `vfeeds.index_constituent.weight`).
    pub weight: i128,
}

/// Index definition + state.
pub struct IndexEngine {
    pub method: IndexMethod,
    /// Outlier rejection threshold in basis points from the median
    /// (0 = disabled).
    pub outlier_bps: i64,
    /// Current divisor (adjusted at rebalancing).
    pub divisor: f64,
    /// Last computed index value.
    pub last_value: Option<f64>,
}

impl IndexEngine {
    pub fn new(method: IndexMethod, divisor: f64, outlier_bps: i64) -> Self {
        IndexEngine {
            method,
            outlier_bps,
            divisor,
            last_value: None,
        }
    }

    /// Compute the index from constituent ticks. Returns the new value.
    ///
    /// Returns None when no usable constituent remains (all zero-weight or
    /// all rejected as outliers).
    pub fn compute(&mut self, ticks: &[ConstituentTick]) -> Option<f64> {
        if ticks.is_empty() || self.divisor <= 0.0 {
            return None;
        }
        // 1. Base aggregate per method.
        let base = match self.method {
            IndexMethod::Median => {
                let mut prices: Vec<f64> = ticks.iter().map(|t| t.price as f64).collect();
                prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
                median(&prices)
            }
            IndexMethod::VolumeWeighted => {
                // Σ price×volume×weight / Σ volume×weight.
                let mut num = 0.0f64;
                let mut den = 0.0f64;
                for t in ticks {
                    let v = if t.volume > 0 { t.volume as f64 } else { 1.0 };
                    num += t.price as f64 * v * t.weight as f64;
                    den += v * t.weight as f64;
                }
                if den == 0.0 {
                    None
                } else {
                    Some(num / den)
                }
            }
            IndexMethod::TimeWeighted => {
                // Σ price×weight / Σ weight.
                let mut num = 0.0f64;
                let mut den = 0.0f64;
                for t in ticks {
                    num += t.price as f64 * t.weight as f64;
                    den += t.weight as f64;
                }
                if den == 0.0 {
                    None
                } else {
                    Some(num / den)
                }
            }
        }?;

        // 2. Outlier rejection: drop constituents deviating > outlier_bps
        // from the median, then recompute once.
        let value = if self.outlier_bps > 0 {
            let mut med_prices: Vec<f64> = ticks.iter().map(|t| t.price as f64).collect();
            med_prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let med = median(&med_prices)?;
            let tol = med * self.outlier_bps as f64 / 10_000.0;
            let kept: Vec<&ConstituentTick> = ticks
                .iter()
                .filter(|t| (t.price as f64 - med).abs() <= tol)
                .collect();
            if kept.is_empty() {
                base
            } else {
                let kept_owned: Vec<ConstituentTick> = kept.into_iter().copied().collect();
                match self.method {
                    IndexMethod::Median => {
                        let mut prices: Vec<f64> =
                            kept_owned.iter().map(|t| t.price as f64).collect();
                        prices.sort_by(|a, b| a.partial_cmp(b).unwrap());
                        median(&prices)?
                    }
                    IndexMethod::VolumeWeighted => {
                        let mut num = 0.0f64;
                        let mut den = 0.0f64;
                        for t in &kept_owned {
                            let v = if t.volume > 0 { t.volume as f64 } else { 1.0 };
                            num += t.price as f64 * v * t.weight as f64;
                            den += v * t.weight as f64;
                        }
                        if den == 0.0 {
                            base
                        } else {
                            num / den
                        }
                    }
                    IndexMethod::TimeWeighted => {
                        let mut num = 0.0f64;
                        let mut den = 0.0f64;
                        for t in &kept_owned {
                            num += t.price as f64 * t.weight as f64;
                            den += t.weight as f64;
                        }
                        if den == 0.0 {
                            base
                        } else {
                            num / den
                        }
                    }
                }
            }
        } else {
            base
        };

        // 3. Apply the divisor.
        let idx = value / self.divisor;
        self.last_value = Some(idx);
        Some(idx)
    }

    /// Rebalance: adjust the divisor so the index value is continuous across
    /// a constituent-set change. Call with the ticks of the OLD set computed
    /// at the same instant as the NEW set's first tick.
    ///
    /// `old_value` = index value before the rebalance. The divisor becomes
    /// `old_value / new_raw_value` so the new set yields the same index level.
    pub fn rebalance_divisor(&mut self, new_raw_value: f64, old_index_value: f64) -> f64 {
        if new_raw_value > 0.0 {
            self.divisor = new_raw_value / old_index_value;
        }
        self.divisor
    }

    pub fn value(&self) -> Option<f64> {
        self.last_value
    }
}

fn median(sorted: &[f64]) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let mid = sorted.len() / 2;
    Some(if sorted.len() % 2 == 1 {
        sorted[mid]
    } else {
        (sorted[mid - 1] + sorted[mid]) / 2.0
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(id: u64, price: i128, volume: i128, weight: i128) -> ConstituentTick {
        ConstituentTick {
            security_id: id,
            price,
            volume,
            weight,
        }
    }

    #[test]
    fn median_method_robust_to_outliers() {
        let mut e = IndexEngine::new(IndexMethod::Median, 1.0, 0);
        // 5 constituents; one wild.
        let ticks = vec![
            t(1, 10_000, 0, 1),
            t(2, 10_050, 0, 1),
            t(3, 10_100, 0, 1),
            t(4, 10_150, 0, 1),
            t(5, 50_000, 0, 1), // outlier
        ];
        // Median of {10_000, 10_050, 10_100, 10_150, 50_000} = 10_100.
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_100.0);
    }

    #[test]
    fn median_even_count() {
        let mut e = IndexEngine::new(IndexMethod::Median, 1.0, 0);
        let ticks = vec![t(1, 10_000, 0, 1), t(2, 10_200, 0, 1)];
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_100.0);
    }

    #[test]
    fn volume_weighted_method() {
        let mut e = IndexEngine::new(IndexMethod::VolumeWeighted, 1.0, 0);
        let ticks = vec![
            t(1, 10_000, 100, 1), // contributes 1_000_000
            t(2, 10_200, 300, 1), // contributes 3_060_000
        ];
        // (10_000×100 + 10_200×300) / 400 = 10_150.
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_150.0);
    }

    #[test]
    fn time_weighted_uses_weights() {
        let mut e = IndexEngine::new(IndexMethod::TimeWeighted, 1.0, 0);
        let ticks = vec![
            t(1, 10_000, 0, 1), // 10_000 × 1
            t(2, 10_400, 0, 3), // 10_400 × 3
        ];
        // (10_000 + 31_200) / 4 = 10_300.
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_300.0);
    }

    #[test]
    fn divisor_scales_the_index() {
        let mut e = IndexEngine::new(IndexMethod::TimeWeighted, 2.0, 0);
        let ticks = vec![t(1, 10_000, 0, 1), t(2, 10_400, 0, 1)];
        let v = e.compute(&ticks).unwrap();
        // Raw aggregate 10_200 ÷ divisor 2 = 5_100.
        assert_eq!(v, 5_100.0);
    }

    #[test]
    fn outlier_rejection_drops_deviants() {
        // Outlier rejection at 1000 bps (10%) from the median.
        let mut e = IndexEngine::new(IndexMethod::Median, 1.0, 1_000);
        let ticks = vec![
            t(1, 10_000, 0, 1),
            t(2, 10_050, 0, 1),
            t(3, 10_100, 0, 1),
            t(4, 50_000, 0, 1), // > 10% away from median 10_050
        ];
        // Median with all 4: (10_050+10_100)/2 = 10_075. Outlier 50_000 is
        // dropped; recompute median of {10_000, 10_050, 10_100} = 10_050.
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_050.0);
    }

    #[test]
    fn outlier_all_rejected_falls_back_to_base() {
        // Degenerate: two constituents 300% apart with a 1% tolerance —
        // everything deviates from the median; fallback to the base median.
        let mut e = IndexEngine::new(IndexMethod::Median, 1.0, 100);
        let ticks = vec![t(1, 10_000, 0, 1), t(2, 40_000, 0, 1)];
        let v = e.compute(&ticks).unwrap();
        // Median of the pair = 25_000 (base); rejection finds nothing within
        // 1% of 25_000 → keeps base.
        assert_eq!(v, 25_000.0);
    }

    #[test]
    fn rebalance_keeps_continuity() {
        let mut e = IndexEngine::new(IndexMethod::TimeWeighted, 1.0, 0);
        // Old set value: 10_200.
        let old = e
            .compute(&[t(1, 10_000, 0, 1), t(2, 10_400, 0, 1)])
            .unwrap();
        assert_eq!(old, 10_200.0);
        // Constituent replaced: new raw aggregate = 9_800 (before divisor).
        // Rebalance the divisor so the index stays at 10_200.
        let new_divisor = e.rebalance_divisor(9_800.0, old);
        assert_eq!(new_divisor, 9_800.0 / 10_200.0);
        // New set at the same level → index unchanged.
        let v = e.compute(&[t(3, 9_800, 0, 1)]).unwrap();
        approx_eq(v, 10_200.0, 1e-9);
    }

    #[test]
    fn empty_ticks_none() {
        let mut e = IndexEngine::new(IndexMethod::Median, 1.0, 0);
        assert!(e.compute(&[]).is_none());
    }

    #[test]
    fn zero_divisor_none() {
        let mut e = IndexEngine::new(IndexMethod::Median, 0.0, 0);
        let ticks = vec![t(1, 10_000, 0, 1)];
        assert!(e.compute(&ticks).is_none());
    }

    #[test]
    fn volume_weighted_zero_volume_falls_to_weight() {
        // Constituents with no volume available weight at 1 like time-weighted.
        let mut e = IndexEngine::new(IndexMethod::VolumeWeighted, 1.0, 0);
        let ticks = vec![t(1, 10_000, 0, 1), t(2, 10_200, 0, 1)];
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_100.0);
    }

    #[test]
    fn weights_respected_in_volume_weighted() {
        let mut e = IndexEngine::new(IndexMethod::VolumeWeighted, 1.0, 0);
        let ticks = vec![
            t(1, 10_000, 100, 1),
            t(2, 10_000, 100, 3), // 3× weight → dominates
            t(3, 10_400, 100, 1),
        ];
        // num = 10_000×100 + 10_000×300 + 10_400×100 = 5_040_000
        // den = 100 + 300 + 100 = 500 → 10_080.
        let v = e.compute(&ticks).unwrap();
        assert_eq!(v, 10_080.0);
    }

    fn approx_eq(a: f64, b: f64, eps: f64) {
        assert!((a - b).abs() < eps, "{a} vs {b}");
    }
}
