-- The quality rules on hourly stations (the official air-quality files). flag_readings was
-- written for readings every 10 seconds: a reading had its 15 following neighbours within
-- minutes, and a 5-minute margin covered its 15 preceding ones. An hourly reading arrives
-- with no following neighbour yet, so a rising value (15, 15, 15, then 50: the start of a
-- plume) was compared with the preceding hours only and flagged as a spike for good, since
-- flags are never removed. Now:
--
-- pm_spike  is judged only once at least 3 following readings exist; until then the
--           reading is shown as received. With 10-second readings that is 30 seconds.
-- lookback  12 hours (job configuration): an hourly reading arrives about 1.5 h after its
--           hour and its third follower 3 hours later, still inside the lookback, so it
--           is judged then.
-- context   readings are checked from the oldest one received in the lookback onward; the
--           windows also read the 16 hours before it (15 hourly readings), which are
--           context only and not flagged again.
--
-- pm_stuck (30 identical values in a row) and station_health.pm10_stuck (10 readings in
-- 30 minutes) count readings: at one reading per hour they never fire in practice. They
-- stay as they are; a counter hung for 30 hours on a value would still be flagged.
--
-- implausible_weather is unchanged from 008.
create or replace procedure flag_readings(job_id integer, config jsonb)
language plpgsql
as $$
declare
    lookback interval := coalesce((config ->> 'lookback')::interval, interval '12 hours');
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
        where ts > oldest - interval '16 hours'
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
                   / nullif(count(pm10_ugm3) over around - 1, 0) as neighbours_mean,
               count(pm10_ugm3) over following as following_count
        from runs
        window around as (partition by station_id order by ts
                          rows between 15 preceding and 15 following),
               following as (partition by station_id order by ts
                             rows between 1 following and 15 following)
    ),
    checked as (
        select * from measured where ts >= oldest
    )
    select station_id, ts, 'pm_stuck' from checked
    where pm10_ugm3 is not null and run_length >= 30
    union all
    select station_id, ts, 'pm_spike' from checked
    where pm10_ugm3 > 30 and pm10_ugm3 > 3 * neighbours_mean and following_count >= 3
    union all
    select station_id, ts, 'implausible_weather' from checked
    where temperature_c not between 5 and 40
       or humidity_pct < 25
       or pressure_hpa not between 950 and 1040
    on conflict do nothing;
end
$$;

select alter_job(job_id, config => '{"lookback": "12 hours"}')
from timescaledb_information.jobs
where proc_name = 'flag_readings';
