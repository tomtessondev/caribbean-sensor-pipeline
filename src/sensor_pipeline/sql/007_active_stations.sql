-- Stations of the active source only. A database that switched source (simulated, then
-- Météo-France) keeps the other source's stations and their history, but they must not
-- show as offline forever on the map, in the fleet views and in the offline alert.
-- `sensor-pipeline migrate` sets the flag: true for the stations of STATION_SOURCE, false
-- for every other one.
alter table stations add column active boolean not null default true;

-- station_health as in 005, active stations only.
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
left join window_24h w using (station_id)
where s.active;

-- station_latest as in 006, active stations only.
create or replace view station_latest as
select
    s.station_id,
    s.town                                                         as name,
    s.territory,
    s.latitude                                                     as lat,
    s.longitude                                                    as lon,
    l.ts,
    l.temperature_c,
    l.humidity_pct,
    l.pressure_hpa,
    l.pm10_ugm3,
    case
        when l.ts > now() - make_interval(secs => s.offline_after_s) then 'online'
        else 'offline'
    end                                                            as status
from stations s
left join lateral (
    select t.ts, t.temperature_c, t.humidity_pct, t.pressure_hpa, t.pm10_ugm3
    from telemetry_clean t
    where t.station_id = s.station_id and t.ts > now() - interval '24 hours'
    order by t.ts desc
    limit 1
) l on true
where s.active;
