-- Where each station's readings come from: the simulator, Météo-France's weather stations
-- or the official particle stations of Gwad'Air and Madininair (LCSQA / INERIS real-time
-- files). `sensor-pipeline migrate` sets it with every station it registers.
alter table stations add column source text not null default 'simulated'
    check (source in ('simulated', 'meteofrance', 'airquality'));

-- Before this migration only two sources existed: every station that is not one of the six
-- simulated ones came from the Météo-France registry.
update stations set source = 'meteofrance'
where station_id not in ('gp-pap-01', 'gp-bte-01', 'gp-mou-01',
                         'mq-fdf-01', 'mq-lam-01', 'mq-san-01');

alter table stations alter column source drop default;

-- station_latest as in 007, plus PM2.5 and the source (new columns go last: CREATE OR
-- REPLACE VIEW keeps the existing ones and the grant to site_reader).
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
    end                                                            as status,
    l.pm2_5_ugm3,
    s.source
from stations s
left join lateral (
    select t.ts, t.temperature_c, t.humidity_pct, t.pressure_hpa, t.pm10_ugm3, t.pm2_5_ugm3
    from telemetry_clean t
    where t.station_id = s.station_id and t.ts > now() - interval '24 hours'
    order by t.ts desc
    limit 1
) l on true
where s.active;
