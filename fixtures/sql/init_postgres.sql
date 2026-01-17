-- Synthetic Helpdesk + Bot source schemas for local ingest demos
CREATE SCHEMA IF NOT EXISTS helpdesk;
CREATE SCHEMA IF NOT EXISTS bot;

CREATE TABLE IF NOT EXISTS helpdesk.tickets (
    id UUID PRIMARY KEY,
    number TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    current_status_name TEXT,
    user_group_name TEXT,
    service_theme TEXT,
    new_channel TEXT
);

CREATE TABLE IF NOT EXISTS helpdesk.ticket_actions (
    id UUID PRIMARY KEY,
    ticket_id UUID REFERENCES helpdesk.tickets(id),
    updated_at TIMESTAMPTZ NOT NULL,
    object_type TEXT NOT NULL,
    status_change JSONB,
    user_group_change JSONB,
    user_name TEXT
);

CREATE TABLE IF NOT EXISTS bot.ai_bot_sessions (
    ticket_id UUID PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL,
    is_classified SMALLINT DEFAULT 0,
    is_answered SMALLINT DEFAULT 0,
    user_continued SMALLINT DEFAULT 0,
    rating DOUBLE PRECISION,
    outcome TEXT
);

CREATE TABLE IF NOT EXISTS bot.ai_answer_dialogs (
    id UUID PRIMARY KEY,
    fsd_item_id UUID,
    handoff_at TIMESTAMPTZ,
    phase TEXT,
    channel TEXT,
    rounds INT DEFAULT 1
);
