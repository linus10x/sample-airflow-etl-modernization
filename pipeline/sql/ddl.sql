-- Idempotent bootstrap for the schemas the pipeline owns. dbt owns staging, intermediate and marts.
create schema if not exists raw;
create schema if not exists ops;

-- One row per distinct file content. The checksum is the identity, so the same bytes are never
-- loaded twice, wherever they are dropped. A corrected re-send has a new checksum and loads.
create table if not exists ops.file_manifest (
    checksum          text primary key,
    path              text        not null,
    feed              text        not null,
    file_date         date        not null,
    status            text        not null check (status in ('loaded', 'quarantined')),
    contract_version  text,
    reason_code       text,
    reason            text,
    row_count         integer     not null default 0,
    size_bytes        integer     not null,
    run_id            text        not null,
    processed_at      timestamptz not null default now()
);
create index if not exists file_manifest_feed_date on ops.file_manifest (feed, file_date);

create table if not exists raw.records (
    checksum  text    not null references ops.file_manifest (checksum),
    ordinal   integer not null,
    feed      text    not null,
    payload   jsonb   not null,
    primary key (checksum, ordinal)
);
