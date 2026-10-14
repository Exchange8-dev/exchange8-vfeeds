-- SPDX-FileCopyrightText: Copyright Logically Magic Inc. <hello@logicallymagic.com>
-- SPDX-FileCopyrightText: Copyright Hamid Jahanzad <Hamid@Jahanzad.com>
-- SPDX-FileCopyrightText: Copyright Contributors to the Exchange8:Vfeeds
-- SPDX-License-Identifier: Apache-2.0

-- Ex8:VFeeds — external venue feed configs, previous-day summary, indices.
--
-- Design: design/sentin8-exchange8-data-model-technical-design.md §3.2 (VF) + §3.9.
-- No migration code (new platform): `sentin8-cli vertical init` applies this.

CREATE SCHEMA IF NOT EXISTS vfeeds;

-- Venue feed configs (legacy EXTERNAL_FEED).
CREATE TABLE IF NOT EXISTS vfeeds.external_feed (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    adapter     TEXT NOT NULL CHECK (adapter IN
                ('stamp_cbbo','stamp_quotes','itch_orders','xmt','soup_tcp',
                 'mold_udp','fix','websocket')),
    transport   TEXT NOT NULL,
    params      JSONB NOT NULL DEFAULT '{}',
    creds_secret_ref TEXT,               -- Sentin8:Secrets reference — never plaintext
    open_time   TIME,
    close_time  TIME,
    enabled     BOOLEAN NOT NULL DEFAULT TRUE
);

-- Recovery endpoints (legacy retransmission feeds).
CREATE TABLE IF NOT EXISTS vfeeds.external_retrans_feed (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    feed_id     UUID NOT NULL REFERENCES vfeeds.external_feed(id),
    address     TEXT NOT NULL,
    port        INTEGER NOT NULL,
    timeout_ms  INTEGER NOT NULL DEFAULT 5000
);

-- Previous-day close summary + lot policy (legacy EQUITY_SUMMARY + Quote PDS).
CREATE TABLE IF NOT EXISTS vfeeds.previous_day_summary (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    security_id UUID NOT NULL,
    trade_date  DATE NOT NULL,
    close_price NUMERIC(18,8) NOT NULL,
    volume      NUMERIC(18,8) NOT NULL DEFAULT 0,
    lot_policy  TEXT NOT NULL DEFAULT 'board' CHECK (lot_policy IN
                ('board','odd','all')),
    UNIQUE (env_id, security_id, trade_date)
);

-- Index definition (method, divisor) — index-engine design.
CREATE TABLE IF NOT EXISTS vfeeds.index (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    name        TEXT NOT NULL,
    method      TEXT NOT NULL DEFAULT 'median'
                CHECK (method IN ('median','volume_weighted',
                                  'time_weighted')),
    divisor     NUMERIC(20,8) NOT NULL DEFAULT 1.0,
    UNIQUE (env_id, name)
);

-- Index constituents.
CREATE TABLE IF NOT EXISTS vfeeds.index_constituent (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    index_id    UUID NOT NULL REFERENCES vfeeds.index(id),
    security_id UUID NOT NULL,
    venue_mic   TEXT,
    weight      NUMERIC(18,8) NOT NULL DEFAULT 1.0,
    UNIQUE (index_id, security_id, venue_mic)
);

-- Index value history (audit + settlement).
CREATE TABLE IF NOT EXISTS vfeeds.index_value (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id      UUID NOT NULL,
    index_id    UUID NOT NULL REFERENCES vfeeds.index(id),
    value       NUMERIC(20,8) NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_exchange8_idxval_env_idx_ts ON vfeeds.index_value (env_id, index_id, ts);

-- ---------------------------------------------------------------------------
-- RLS (architecture-principles.md §4.3): enable + force on all tenant tables.
-- Policies use the platform helpers sentin8_context_initialized() and
-- sentin8_current_env_id(); services call set_tenant_context on the same
-- connection as the data query.
-- ---------------------------------------------------------------------------
DO $$
DECLARE tbl TEXT;
BEGIN
    FOREACH tbl IN ARRAY ARRAY['vfeeds.external_feed', 'vfeeds.external_retrans_feed', 'vfeeds.previous_day_summary', 'vfeeds.index', 'vfeeds.index_constituent', 'vfeeds.index_value'] LOOP
        BEGIN
            EXECUTE format('ALTER TABLE %s ENABLE ROW LEVEL SECURITY', tbl);
            EXECUTE format('ALTER TABLE %s FORCE ROW LEVEL SECURITY', tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_select ON %2$s;
CREATE POLICY %1$I_select ON %2$s FOR SELECT USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_insert ON %2$s;
CREATE POLICY %1$I_insert ON %2$s FOR INSERT WITH CHECK (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_update ON %2$s;
CREATE POLICY %1$I_update ON %2$s FOR UPDATE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
            EXECUTE format('DROP POLICY IF EXISTS %1$I_delete ON %2$s;
CREATE POLICY %1$I_delete ON %2$s FOR DELETE USING (sentin8_context_initialized() AND env_id = sentin8_current_env_id())', split_part(tbl, '.', 2), tbl);
        EXCEPTION WHEN OTHERS THEN
            RAISE NOTICE 'RLS setup failed for %: %', tbl, SQLERRM;
        END;
    END LOOP;
END $$;

-- ---------------------------------------------------------------------------
-- Per-service role grants (schema-per-service: this service owns schema
-- `vfeeds`; grants live in db `exchange8` only — design doc §1).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_vfeeds_service') THEN
        ALTER ROLE exchange8_vfeeds_service SET search_path = vfeeds, public;
        GRANT USAGE ON SCHEMA vfeeds TO exchange8_vfeeds_service;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA vfeeds TO exchange8_vfeeds_service;
        GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA vfeeds TO exchange8_vfeeds_service;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Local outbox (DBX §6.7 — generated from `sentin8_outbox::outbox_table_sql(
-- "vfeeds")`): rows written in the same DB transaction as the domain change;
-- the shell's relay (boot step §8b) pushes them to Bus `POST /v1/produce`.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vfeeds.outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    env_id          UUID NOT NULL,
    event_type      TEXT NOT NULL,
    stream_family   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    publisher_id_token  TEXT,
    publisher_subject   TEXT,
    relayed         BOOLEAN NOT NULL DEFAULT false,
    relayed_at      TIMESTAMPTZ,
    relay_attempts  INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_vfeeds_outbox_unrelayed
    ON vfeeds.outbox (created_at) WHERE relayed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE vfeeds.outbox ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE vfeeds.outbox FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS outbox_select ON vfeeds.outbox';
    EXECUTE 'CREATE POLICY outbox_select ON vfeeds.outbox FOR SELECT
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_insert ON vfeeds.outbox';
    EXECUTE 'CREATE POLICY outbox_insert ON vfeeds.outbox FOR INSERT
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
    EXECUTE 'DROP POLICY IF EXISTS outbox_update ON vfeeds.outbox';
    EXECUTE 'CREATE POLICY outbox_update ON vfeeds.outbox FOR UPDATE
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for vfeeds.outbox: %', SQLERRM;
END $$;

CREATE OR REPLACE FUNCTION vfeeds.vfeeds_notify_outbox() RETURNS TRIGGER
LANGUAGE plpgsql AS $fn$
BEGIN
    PERFORM pg_notify('vfeeds_outbox_changed', '');
    RETURN NEW;
END;
$fn$;
DROP TRIGGER IF EXISTS outbox_notify_trigger ON vfeeds.outbox;
CREATE TRIGGER outbox_notify_trigger
    AFTER INSERT ON vfeeds.outbox
    FOR EACH ROW
    EXECUTE FUNCTION vfeeds.vfeeds_notify_outbox();

-- ---------------------------------------------------------------------------
-- Local audit journal (DBX §6.8 — generated from
-- `sentin8_audit::audit_journal_sql("vfeeds")`): rows written in the same
-- transaction as the domain change; the shell's pusher (boot step §8c) pushes
-- them to Sys `POST /v1/audit` (`sentin8:sys:audit:write`). A Sys outage
-- never drops audit.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS vfeeds.audit_journal (
    id              UUID PRIMARY KEY,
    env_id          UUID NOT NULL,
    action          TEXT NOT NULL,
    target_resource_type TEXT,
    target_resource_id    TEXT,
    actor_user_id   TEXT,
    actor_subject_type TEXT,
    severity        TEXT NOT NULL DEFAULT 'info',
    correlation_id  TEXT,
    before_state    JSONB,
    after_state     JSONB,
    details         JSONB,
    pushed          BOOLEAN NOT NULL DEFAULT false,
    pushed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_vfeeds_audit_journal_unpushed
    ON vfeeds.audit_journal (created_at) WHERE pushed = false;
DO $$ BEGIN
    EXECUTE 'ALTER TABLE vfeeds.audit_journal ENABLE ROW LEVEL SECURITY';
    EXECUTE 'ALTER TABLE vfeeds.audit_journal FORCE ROW LEVEL SECURITY';
    EXECUTE 'DROP POLICY IF EXISTS audit_journal_dml ON vfeeds.audit_journal';
    EXECUTE 'CREATE POLICY audit_journal_dml ON vfeeds.audit_journal
        USING (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())
        WITH CHECK (sentin8_context_initialized()
               AND env_id = sentin8_current_env_id())';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'RLS setup failed for vfeeds.audit_journal: %', SQLERRM;
END $$;

-- Outbox + journal role grants (same pattern as the table grants above).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'exchange8_vfeeds_service') THEN
        GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA vfeeds TO exchange8_vfeeds_service;
    END IF;
END $$;
