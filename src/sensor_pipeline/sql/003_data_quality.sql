-- Data quality: suspect readings are flagged, never deleted. The raw table keeps what the
-- stations sent; dashboards read the validated view; the flags explain every exclusion.

create table reading_flags (
    station_id  text not null,
    ts          timestamptz not null,
    flag        text not null check (flag in ('pm_stuck', 'pm_spike', 'implausible_weather')),
    detected_at timestamptz not null default now(),
    primary key (station_id, ts, flag)
);
select create_hypertable('reading_flags', by_range('ts', interval '7 days'));
select add_retention_policy('reading_flags', interval '180 days');

-- Readings are checked by arrival, not by timestamp: a station replaying its buffer after
-- an outage sends readings hours old, and they must be checked too.
create index telemetry_received_at_idx on telemetry (received_at);

-- The three rules, over every reading received in the last `lookback` (default 3 hours),
-- whatever its timestamp, plus the readings around it. Idempotent: run it as often as
-- needed, a reading is flagged once per rule.
--
-- pm_stuck            30 or more consecutive identical PM10 values: a working particle
--                     counter is never that steady (the fan or the serial link has hung).
-- pm_spike            PM10 above 3 times the mean of the 30 surrounding readings and above
--                     30 µg/m³. A Saharan plume rises over hours, so its neighbours rise
--                     with it and it is not flagged; a lone electrical glitch is.
-- implausible_weather Possible for the sensor, not for a sea-level Caribbean town:
--                     temperature outside 15-40 °C, humidity under 25 %, pressure outside
--                     950-1040 hPa (a major hurricane goes below 950 and must be checked
--                     by a human, not trusted blindly either way).
create procedure flag_readings(job_id integer, config jsonb)
language plpgsql
as $$
declare
    lookback interval := coalesce((config ->> 'lookback')::interval, interval '3 hours');
    oldest timestamptz;
begin
    select min(ts) into oldest from telemetry where received_at > now() - lookback;
    if oldest is null then
        return;  -- nothing arrived
    end if;

    insert into reading_flags (station_id, ts, flag)
    with recent as (
        select station_id, ts, pm10_ugm3, temperature_c, humidity_pct, pressure_hpa,
               lag(pm10_ugm3) over (partition by station_id order by ts) as previous_pm10
        from telemetry
        -- 5 minutes of margin so the windows of the oldest readings are complete
        where ts > oldest - interval '5 minutes'
    ),
    runs as (
        select *,
               sum(case when pm10_ugm3 is distinct from previous_pm10 then 1 else 0 end)
                   over (partition by station_id order by ts) as run_id
        from recent
    ),
    measured as (
        select *,
               count(*) over (partition by station_id, run_id) as run_length,
               (sum(pm10_ugm3) over around - pm10_ugm3)
                   / nullif(count(pm10_ugm3) over around - 1, 0) as neighbours_mean
        from runs
        window around as (partition by station_id order by ts
                          rows between 15 preceding and 15 following)
    )
    select station_id, ts, 'pm_stuck' from measured
    where pm10_ugm3 is not null and run_length >= 30
    union all
    select station_id, ts, 'pm_spike' from measured
    where pm10_ugm3 > 30 and pm10_ugm3 > 3 * neighbours_mean
    union all
    select station_id, ts, 'implausible_weather' from measured
    where temperature_c not between 15 and 40
       or humidity_pct < 25
       or pressure_hpa not between 950 and 1040
    on conflict do nothing;
end
$$;

select add_job('flag_readings', interval '5 minutes', config => '{"lookback": "3 hours"}');

-- Readings as the dashboards should see them: a flagged value becomes NULL, the rest of
-- the reading stays. A stuck PM sensor does not invalidate the temperature.
create view telemetry_clean as
select
    t.station_id,
    t.ts,
    t.seq,
    case when f.weather_suspect then null else t.temperature_c end as temperature_c,
    case when f.weather_suspect then null else t.humidity_pct end  as humidity_pct,
    case when f.weather_suspect then null else t.pressure_hpa end  as pressure_hpa,
    case when f.pm_suspect then null else t.pm2_5_ugm3 end         as pm2_5_ugm3,
    case when f.pm_suspect then null else t.pm10_ugm3 end          as pm10_ugm3,
    t.battery_v,
    t.rssi_dbm
from telemetry t
left join lateral (
    select
        coalesce(bool_or(flag in ('pm_stuck', 'pm_spike')), false) as pm_suspect,
        coalesce(bool_or(flag = 'implausible_weather'), false)     as weather_suspect
    from reading_flags rf
    where rf.station_id = t.station_id and rf.ts = t.ts
) f on true;

-- One number per station and local day: the share of expected readings that arrived and
-- passed every rule. Expected = received + lost, the losses read from sequence gaps.
create view station_quality_daily as
with readings as (
    select
        station_id,
        ts,
        time_bucket('1 day', ts, 'America/Guadeloupe') as day,
        greatest(coalesce(seq - lag(seq) over (partition by station_id order by ts) - 1, 0), 0)
            as lost
    from telemetry
    where ts > now() - interval '30 days'
),
flagged as (
    select distinct station_id, ts
    from reading_flags
    where ts > now() - interval '30 days'
)
select
    r.station_id,
    r.day,
    count(*)                                                        as received,
    sum(r.lost)                                                     as lost,
    count(f.ts)                                                     as flagged,
    round(100.0 * count(*) / nullif(count(*) + sum(r.lost), 0), 2)  as completeness_pct,
    round(100.0 * (count(*) - count(f.ts)) / count(*), 2)           as valid_pct,
    round(100.0 * (count(*) - count(f.ts)) / nullif(count(*) + sum(r.lost), 0), 1)
                                                                    as quality_score
from readings r
left join flagged f using (station_id, ts)
group by r.station_id, r.day;

grant select on reading_flags, telemetry_clean, station_quality_daily to grafana_reader;
