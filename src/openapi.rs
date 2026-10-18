// SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
// SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
// SPDX-License-Identifier: Apache-2.0

//! OpenAPI spec for Exchange8:Vfeeds.
//!
//! The service mounts no domain routes of its own: the binary's
//! `extend_router` only mounts the shared Shield policy-admin surface
//! (`sentin8::shell::mount_policy_admin`), so the spec lists the
//! `sentin8::shell::policy_*` wrappers plus the shell-owned `/v1/health`.
//! The service exposes `GET /openapi.json` via the shell's OpenAPI route.

use utoipa::OpenApi;

/// Spec wrapper for the shell-owned `/v1/health` mount.
#[utoipa::path(
    get,
    path = "/v1/health",
    responses(
        (status = 200, description = "Service is healthy", body = serde_json::Value),
    ),
    security(),
    tag = "vfeeds",
)]
#[allow(unused)]
async fn health() {}

/// OpenAPI spec for the vfeeds API.
#[derive(OpenApi)]
#[openapi(
    paths(
        crate::openapi::health,
        sentin8::shell::policy_list_sets,
        sentin8::shell::policy_get_set,
        sentin8::shell::policy_put_set,
        sentin8::shell::policy_delete_set,
        sentin8::shell::policy_test_set,
        sentin8::shell::policy_mode_enforce,
        sentin8::shell::policy_mode_shadow,
        sentin8::shell::policy_mode_disable,
        sentin8::shell::policy_create_set,
        sentin8::shell::policy_save_set,
        sentin8::shell::policy_flip_set,
        sentin8::shell::policy_list_decisions,
        sentin8::shell::policy_delete_relation_path,
        sentin8::shell::policy_delete_attribute_path,
        sentin8::shell::policy_list_relations,
        sentin8::shell::policy_put_relation,
        sentin8::shell::policy_delete_relation,
        sentin8::shell::policy_check_relation,
        sentin8::shell::policy_list_attributes,
        sentin8::shell::policy_put_attribute,
        sentin8::shell::policy_delete_attribute,
    ),
    components(schemas()),
    security(),
    tags(
        (name = "vfeeds", description = "Exchange8:Vfeeds — policy admin + health only"),
        (name = "policy", description = "Shield policy administration (shared surface)"),
    ),
)]
pub struct VfeedsOpenApi;
