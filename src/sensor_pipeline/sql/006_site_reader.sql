-- What the portfolio site's map shows: one row per station with its latest clean reading
-- of the last 24 hours (flagged values are NULL, as in telemetry_clean) and whether it is
-- online by its own threshold. Nothing else: no battery, signal, sequence or flags.
create view station_latest as
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
) l on true;

-- The public endpoint reads through its own role: this view and nothing else (the view
-- reads the tables with its owner's rights). The login and its password are set by
-- `sensor-pipeline migrate --site-password`, never here.
do $$
begin
    if not exists (select from pg_roles where rolname = 'site_reader') then
        create role site_reader nologin;
    end if;
end
$$;

grant usage on schema public to site_reader;
grant select on station_latest to site_reader;
