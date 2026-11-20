# Grafana for docker-compose.vps.yml with the provisioning and dashboards baked in: the
# deployment needs no file from the host. The dashboards live outside /var/lib/grafana so
# that the data volume mounted there never hides a newer version of them.
FROM grafana/grafana:12.3.0

COPY grafana/provisioning /etc/grafana/provisioning
COPY grafana/dashboards /etc/grafana/dashboards
