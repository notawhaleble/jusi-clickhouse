CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.accounts
(
    id UInt64,
    email String,
    display_name String,
    active Bool,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY id;

CREATE TABLE IF NOT EXISTS demo.events
(
    id UInt64,
    account_id UInt64,
    event_type LowCardinality(String),
    payload String,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY (account_id, id);

CREATE TABLE IF NOT EXISTS demo.blob_files
(
    id UInt64,
    filename String,
    content_type String,
    payload String,
    created_at DateTime
)
ENGINE = MergeTree
ORDER BY id;

TRUNCATE TABLE demo.accounts;
TRUNCATE TABLE demo.events;
TRUNCATE TABLE demo.blob_files;

INSERT INTO demo.accounts (id, email, display_name, active, created_at) VALUES
    (1, 'ada@example.test', 'Ada Lovelace', true, now() - INTERVAL 3 DAY),
    (2, 'grace@example.test', 'Grace Hopper', true, now() - INTERVAL 2 DAY),
    (3, 'alan@example.test', 'Alan Turing', false, now() - INTERVAL 1 DAY);

INSERT INTO demo.events (id, account_id, event_type, payload, created_at) VALUES
    (1, 1, 'login', '{"ip":"127.0.0.1"}', now() - INTERVAL 3 HOUR),
    (2, 1, 'query', '{"rows":42}', now() - INTERVAL 2 HOUR),
    (3, 2, 'login', '{"ip":"127.0.0.2"}', now() - INTERVAL 1 HOUR),
    (4, 3, 'disabled_login', '{"reason":"inactive"}', now());

INSERT INTO demo.blob_files (id, filename, content_type, payload, created_at) VALUES
    (
        1,
        'test-blob.zip',
        'application/zip',
        unhex('504b03040a0000000000231ab65c2909ab311b0000001b0000000f001c00626c6f622d726561646d652e747874555409000382a00f6a82a00f6a75780b000104f501000004140000004a75736920506f73746772657320626c6f6220666978747572650a504b01021e030a0000000000231ab65c2909ab311b0000001b0000000f0018000000000001000000a48100000000626c6f622d726561646d652e747874555405000382a00f6a75780b000104f50100000414000000504b0506000000000100010055000000640000000000'),
        now()
    );

CREATE OR REPLACE VIEW demo.account_summary AS
SELECT
    a.id,
    a.email,
    a.display_name,
    a.active,
    count(e.id) AS event_count,
    max(e.created_at) AS last_event_at
FROM demo.accounts AS a
LEFT JOIN demo.events AS e ON e.account_id = a.id
GROUP BY
    a.id,
    a.email,
    a.display_name,
    a.active;
