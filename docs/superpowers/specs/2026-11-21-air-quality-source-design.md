# Official PM10 / PM2.5 as a second real source — design

## Intent

The sensor pipeline was designed for air-quality stations, but its live demo currently
carries only Météo-France weather stations (no particles). The owner gave carte blanche
(2026-11-21) to add the official hourly particle measurements of Gwad'Air (Guadeloupe,
Saint-Martin) and Madininair (Martinique) as a second source of the same bridge, so the
PM part of the project (contract fields, quality rules, dashboards) runs on real data.

## Source (verified 2026-11-21)

- LCSQA / INERIS real-time E2 files on data.gouv.fr, no key, Licence Ouverte 2.0:
  `https://files.data.gouv.fr/ineris/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/<YYYY>/FR_E2_<YYYY-MM-DD>.csv`
  (302 to an OVH S3 object with `Last-Modified`; follow redirects).
- One file per **local** day for all of France (≈ 12 MB, ≈ 47 000 rows), rewritten every
  hour around hh:30 UTC; the previous 2-3 days are rewritten too.
- CSV: UTF-8 with BOM, `;`, quoted values, some empty unquoted fields (use the csv
  module). Columns used: `Date de début`, `Organisme` (`GWAD'AIR`, `MADININAIR`),
  `code site`, `nom site`, `Polluant` (`PM10`, `PM2.5`), `valeur`, `unité de mesure`
  (`µg-m3` or `µg/m3`), `code qualité` (`A` valid), `validité` (`1` valid).
- **Timestamps are local time of the territory** (UTC−4 in the Antilles, no DST), despite
  the XML saying `+00:00` (proved against Gwad'Air's ArcGIS service in true UTC).
- Values are raw, not yet validated (`moyenne horaire brute`): the site says so.
- Latency ≈ 1.5 h at publication, ≈ 2.5 h at worst.

Stations (coordinates from the same dataset's "Dataset D", WGS84):

| Code | Name | Lat | Lon | Pollutants |
| --- | --- | --- | --- | --- |
| FR37001 | Baie-Mahault Belcourt | 16.257307 | -61.591724 | PM10 |
| FR37006 | Pointe-à-Pitre Victoire | 16.23674 | -61.5341 | PM10, PM2.5 |
| FR37010 | Basse-Terre Pichon | 15.994349 | -61.727203 | PM10, PM2.5 |
| FR37040 | Les Abymes RN1 | 16.254086 | -61.53713 | PM10 |
| FR37041 | Anse-Bertrand Campêche | 16.4545 | -61.4281 | PM10 |
| FR37095 | Saint-Martin Concordia | 18.066668 | -63.076668 | PM10, PM2.5 |
| FR39008 | Schœlcher | 14.616389 | -61.100792 | PM10, PM2.5 |
| FR39009 | Le Lamentin | 14.610653 | -61.002586 | PM10, PM2.5 |
| FR39011 | Fort-de-France Renéville | 14.614111 | -61.05308 | PM10 |
| FR39014 | Sainte-Luce | 14.468869 | -60.927418 | PM10, PM2.5 |
| FR39015 | Fort-de-France Hôtel de ville | 14.607444 | -61.06978 | PM10, PM2.5 |
| FR39016 | Le Robert Bourg | 14.678025 | -60.939056 | PM10, PM2.5 |

## Design (my calls)

- A second registry `data/air_quality_antilles.csv` (same columns as the Météo-France one,
  `source` = `airquality`), ids from the existing commune-code table, never colliding with
  the Météo-France or simulator ids; `offline_after_s` = 14400 (hourly, ≈ 2.5 h latency).
- `STATION_SOURCE=meteofrance` keeps its meaning for the weather stations; the deployed
  stack runs both real sources: a new value `real` (= Météo-France + air quality) is what
  `docker-compose.vps.yml` uses for migrate, provision and the bridge; the `active` flag
  covers both registries.
- Bridge: a second poller every 15 minutes with `If-Modified-Since`, reading the files of
  the current and previous local day, converting local time to UTC with
  `America/Guadeloupe` / `America/Martinique`, one reading per station and hour
  (PM10 and PM2.5 merged; other contract fields null), `ts` = start of the hour in UTC,
  `seq` = hours since the epoch, only rows with `A`/`1` and a value, readings older than
  the 3 h... **no**: hourly data arrives up to 2.5 h late and the files are rewritten for
  3 days, so the "already sent" window for this source is 72 h. Unknown stations are
  skipped once-logged, as for Météo-France. Same MQTT sessions (one per station).
- Endpoint `/v1/stations` adds `pm2_5_ugm3` and `source` (`meteofrance` | `airquality`);
  the site's stations map gets a toggle Température / PM10 and shows air-quality stations
  with their PM values and "données brutes non validées (LCSQA / INERIS, Gwad'Air,
  Madininair)".
- The quality rules already handle PM (stuck, spikes); no change unless a test shows one.

## Left to the builder

Exact parsing and polling code, ids, Grafana panels for the new stations (the air-quality
dashboard should show them).
