-- Each station goes offline after its own silence. The simulator publishes every 10 s, so
-- 5 minutes (the default) is an outage; Météo-France stations are published minutes to an
-- hour late, so the registry gives them 30 or 100 minutes (sensor_pipeline/registry.py).
alter table stations
    add column offline_after_s integer not null default 300 check (offline_after_s > 0);

-- station_health as in 002, plus the threshold and the silence measured against it:
-- offline_ratio > 1 means offline (the "Station offline" alert). New columns go last, as
-- CREATE OR REPLACE VIEW requires.
create or replace view station_health as
with recent as (
    select
        station_id,
        ts,
        pm10_ugm3,
        seq - lag(seq) over (partition by station_id order by ts) - 1 as gap
    from telemetry
    where ts > now() - interval '24 hours'
),
window_24h as (
    select
        station_id,
        count(*)                                                 as received,
        sum(greatest(coalesce(gap, 0), 0))                       as lost,
        count(*) filter (where ts > now() - interval '30 minutes') as readings_30m,
        count(distinct pm10_ugm3) filter (where ts > now() - interval '30 minutes')
                                                                 as pm10_values_30m
    from recent
    group by station_id
),
latest as (
    select distinct on (station_id)
        station_id, ts, battery_v, rssi_dbm, pm10_ugm3
    from telemetry
    where ts > now() - interval '7 days'
    order by station_id, ts desc
)
select
    s.station_id,
    s.town,
    s.territory,
    s.latitude,
    s.longitude,
    coalesce(st.status, 'unknown')                           as status,
    st.changed_at                                            as status_since,
    l.ts                                                     as last_reading_at,
    round((extract(epoch from now() - l.ts) / 60)::numeric, 1) as minutes_since_last_reading,
    l.battery_v,
    l.rssi_dbm,
    l.pm10_ugm3                                              as pm10_latest_ugm3,
    round(100.0 * w.lost / nullif(w.lost + w.received, 0), 2) as loss_pct_24h,
    -- A working particle counter never reports the exact same value for 30 minutes.
    coalesce(w.readings_30m >= 10 and w.pm10_values_30m = 1, false) as pm10_stuck,
    s.offline_after_s,
    round((extract(epoch from now() - l.ts) / s.offline_after_s)::numeric, 2) as offline_ratio
from stations s
left join station_status st using (station_id)
left join latest l using (station_id)
left join window_24h w using (station_id);
