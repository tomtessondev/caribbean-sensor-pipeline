create extension if not exists timescaledb;

create table stations (
    station_id text primary key,
    town       text not null,
    territory  text not null,
    latitude   double precision not null,
    longitude  double precision not null
);

-- One row per reading. The key makes ingestion idempotent: MQTT QoS 1 delivers at least
-- once, so a redelivered message hits the key and is ignored.
create table telemetry (
    station_id    text not null references stations,
    ts            timestamptz not null,
    seq           bigint not null,
    temperature_c double precision,
    humidity_pct  double precision,
    pressure_hpa  double precision,
    pm2_5_ugm3    double precision,
    pm10_ugm3     double precision,
    battery_v     double precision,
    rssi_dbm      integer,
    received_at   timestamptz not null default now(),
    primary key (station_id, ts)
);

-- Partitioned by time into daily chunks behind a single table.
select create_hypertable('telemetry', by_range('ts', interval '1 day'));

-- Chunks older than a week are compressed column by column, per station.
alter table telemetry set (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'station_id',
    timescaledb.compress_orderby = 'ts desc'
);
select add_compression_policy('telemetry', interval '7 days');
select add_retention_policy('telemetry', interval '180 days');

-- Hourly rollup kept up to date by a background job; queries also see the hours not yet
-- materialised (materialized_only = false), so dashboards are never behind.
create materialized view telemetry_hourly
with (timescaledb.continuous, timescaledb.materialized_only = false) as
select
    station_id,
    time_bucket('1 hour', ts) as bucket,
    avg(temperature_c)        as temperature_avg_c,
    avg(humidity_pct)         as humidity_avg_pct,
    avg(pressure_hpa)         as pressure_avg_hpa,
    avg(pm2_5_ugm3)           as pm2_5_avg_ugm3,
    avg(pm10_ugm3)            as pm10_avg_ugm3,
    max(pm10_ugm3)            as pm10_max_ugm3,
    min(battery_v)            as battery_min_v,
    count(*)                  as readings
from telemetry
group by station_id, bucket
with no data;

select add_continuous_aggregate_policy(
    'telemetry_hourly',
    start_offset      => interval '3 days',
    end_offset        => interval '1 hour',
    schedule_interval => interval '15 minutes'
);

-- Last known state of each station, from the retained status topic and its last will.
create table station_status (
    station_id text primary key references stations,
    status     text not null check (status in ('online', 'offline')),
    changed_at timestamptz not null
);

-- Messages that failed validation, kept verbatim for diagnosis instead of being dropped.
create table ingest_errors (
    id          bigserial primary key,
    received_at timestamptz not null,
    topic       text not null,
    payload     text not null,
    error       text not null
);
