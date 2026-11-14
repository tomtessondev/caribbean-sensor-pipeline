# Roadmap

1. **Core pipeline.** Message contract, simulated stations, Mosquitto, ingestion into
   TimescaleDB (hypertable, compression, retention, hourly continuous aggregate), Docker
   Compose, unit and integration tests, CI.
2. **Grafana as code.** Provisioned datasource and dashboards (map of stations, PM10 with
   the EU limit, link quality, battery, message loss from sequence gaps), alert rules
   (dust episode, station offline, stuck sensor).
3. **ESP32 firmware.** PlatformIO, BME280 + PMS5003, Wi-Fi reconnection, last will, an
   offline buffer replayed on reconnection, the same JSON contract; simulated in CI with
   Wokwi.
4. **Data quality.** Detect stuck sensors and spikes in SQL, flag readings instead of
   deleting them, publish a per-station quality score.
5. **Security.** TLS on the broker, one username/password (or client certificate) per
   station, ACLs so a station can only publish on its own topics.
