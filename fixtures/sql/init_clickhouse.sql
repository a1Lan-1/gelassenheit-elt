-- Gelassenheit ELT - ClickHouse databases
CREATE DATABASE IF NOT EXISTS gel_helpdesk;
CREATE DATABASE IF NOT EXISTS gel_devices;
CREATE DATABASE IF NOT EXISTS gel_bot;
CREATE DATABASE IF NOT EXISTS gel_crm;
CREATE DATABASE IF NOT EXISTS gel_pbx;
CREATE DATABASE IF NOT EXISTS gel_dialer;
CREATE DATABASE IF NOT EXISTS gel_workforce;

-- Demo bronze-landed ticket extract (minimal grain for smoke)
CREATE TABLE IF NOT EXISTS gel_helpdesk.demo_tickets
(
    id UUID,
    number String,
    created_at DateTime64(3, 'Europe/Moscow'),
    updated_at DateTime64(3, 'Europe/Moscow'),
    current_status_name String,
    user_group_name String,
    service_theme String,
    new_channel String
)
ENGINE = MergeTree
ORDER BY (created_at, id);

CREATE TABLE IF NOT EXISTS gel_bot.demo_bot_sessions
(
    ticket_id UUID,
    started_at DateTime64(3, 'Europe/Moscow'),
    is_classified UInt8,
    is_answered UInt8,
    user_continued UInt8,
    rating Nullable(Float64),
    outcome String
)
ENGINE = MergeTree
ORDER BY (started_at, ticket_id);
