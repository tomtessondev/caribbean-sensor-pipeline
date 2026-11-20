# The broker of docker-compose.vps.yml with its configuration baked in: the deployment
# needs no file from the host.
FROM eclipse-mosquitto:2.0.22

COPY docker/mosquitto.conf /mosquitto/config/mosquitto.conf
COPY docker/broker-entrypoint.sh /docker/broker-entrypoint.sh

ENTRYPOINT ["/bin/sh", "/docker/broker-entrypoint.sh"]
