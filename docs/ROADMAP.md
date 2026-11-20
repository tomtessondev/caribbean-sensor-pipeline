# Roadmap

1. **Core pipeline** (done). Message contract, simulated stations, Mosquitto, ingestion into
   TimescaleDB (hypertable, compression, retention, hourly continuous aggregate), Docker
   Compose, unit and integration tests, CI.
2. **Grafana as code** (done). Provisioned datasource and dashboards (map of stations, PM10 with
   the EU limit, link quality, battery, message loss from sequence gaps), alert rules
   (dust episode, station offline, stuck sensor).
3. **ESP32 firmware** (done). PlatformIO, BME280 + PMS5003, Wi-Fi reconnection, last will,
   an offline buffer replayed on reconnection, the same JSON contract. CI runs the logic
   tests on the host and builds the firmware; simulating the whole board with Wokwi is
   left for later (it needs an account token).
4. **Data quality** (done). Detect stuck sensors and spikes in SQL, flag readings instead of
   deleting them, publish a per-station quality score.
5. **Security** (done). TLS on the broker, one username/password (or client certificate) per
   station, ACLs so a station can only publish on its own topics.
6. **Real stations** (in progress). A bridge from Météo-France's automatic weather stations
   (Package Observations API) to MQTT, one session per station, that survives API, parsing
   and broker errors and catches up on missed slots up to 3 hours back; a registry of the
   real stations with permanent ids (one three-letter code per commune, none shared with
   the simulator) and an offline threshold per station derived from its observed cadence;
   only the stations of the active source on the map, in the fleet views and in the
   alert; a read-only endpoint serving the latest reading of each station to the portfolio
   site.
