-- implausible_weather took 15 °C as the lowest believable temperature: true for a sea-level
-- Caribbean town, not for the real mountain stations (Saint-Claude at 1100 m, Col des
-- Mamelles at 510 m), whose winter nights go below it and were flagged. The lower bound
-- is now 5 °C, well below what the highest stations of the islands read. Everything else
-- is flag_readings as in 003.
--
-- implausible_weather temperature outside 5-40 °C, humidity under 25 %, pressure outside
--                     950-1040 hPa.
create or replace procedure flag_readings(job_id integer, config jsonb)
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
    where temperature_c not between 5 and 40
       or humidity_pct < 25
       or pressure_hpa not between 950 and 1040
    on conflict do nothing;
end
$$;
