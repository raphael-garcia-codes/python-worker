-- =====================================================================
-- Migration: create processed_events table
-- Equivalent to Alembic revision: 202607250001
-- =====================================================================


-- ============================
-- UPGRADE
-- ============================

-- Required so gen_random_uuid() is available as a column default.
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE processed_events (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source         VARCHAR NOT NULL,
    payload        JSONB NOT NULL,
    status         VARCHAR NOT NULL,
    error_message  TEXT,
    processed_at   TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at     TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now()
);

CREATE INDEX ix_processed_events_source ON processed_events (source);
CREATE INDEX ix_processed_events_status ON processed_events (status);


-- ============================
-- DOWNGRADE
-- ============================

-- DROP INDEX IF EXISTS ix_processed_events_status;
-- DROP INDEX IF EXISTS ix_processed_events_source;
-- DROP TABLE IF EXISTS processed_events;
