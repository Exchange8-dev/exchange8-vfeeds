// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
// SPDX-License-Identifier: Apache-2.0

//! Exchange8:VFEEDS service binary - external venue feeds + CBBO + index engine.
//!
//! Design: `design/sentin8-exchange8-technical-design.md` (see the
//! service doc for the exact file). Core logic in the sibling modules;
//! this shell wires the tokio HTTP/TLS surface via the platform
//! `sentin8-shell` crate (registration, DB pool, health). See
//! `services/vfeeds/DESIGN.md`.

#![allow(dead_code)]

// (see DESIGN.md §7). Workers (bus consumers, session
// supervisors, purge jobs) are in-process tokio tasks, so the
mod adapters;
mod bbo;
mod index;
mod openapi;
mod recovery;
mod registration;

fn extend_router(
    app: axum::Router<std::sync::Arc<sentin8::shell::AppState>>,
    _state: std::sync::Arc<sentin8::shell::AppState>,
) -> axum::Router<std::sync::Arc<sentin8::shell::AppState>> {
    // ── Shield (Sentin8:Policy) admin API - shared implementation;
    // the shell AppState already implements HasPolicyAdmin (see DESIGN.md §2).
    sentin8::shell::mount_policy_admin(
        // Feed adapters are dedicated TCP/multicast tasks (design §4.1);
        // the HTTP router carries health + admin endpoints only.
        app,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    sentin8::shell::serve(
        sentin8::shell::Sentin8Shell::new("exchange8:vfeeds", "EXCHANGE8_VFEEDS")
            .with_registration(registration::print)
            .with_simple_state()
            .with_router(extend_router),
    )
    .await
}
