-- One row per station: what an operator checks first. Used by the fleet dashboard and the
-- "station offline" and "stuck sensor" alerts.
--
-- Loss is measured from sequence gaps: each station numbers its readings, so a jump from
-- 41 to 44 means two readings were lost on the way. A smaller number than the previous
-- one means the device rebooted and restarted at 0: not a loss.
create view station_health as
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
    coalesce(w.readings_30m >= 10 and w.pm10_values_30m = 1, false) as pm10_stuck
from stations s
left join station_status st using (station_id)
left join latest l using (station_id)
left join window_24h w using (station_id);

-- Grafana reads through its own role: select only, on what the dashboards need. The login
-- and its password are set by `sensor-pipeline migrate --grafana-password`, never here.
do $$
begin
    if not exists (select from pg_roles where rolname = 'grafana_reader') then
        create role grafana_reader nologin;
    end if;
end
$$;

grant usage on schema public to grafana_reader;
grant select on stations, telemetry, telemetry_hourly, station_status, ingest_errors,
    station_health to grafana_reader;
