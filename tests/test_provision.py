from __future__ import annotations

import ipaddress
import json
import stat
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives.asymmetric import ec

from sensor_pipeline.provision import INGESTOR_USER, load_credentials, provision_broker

STATIONS = ["gp-pap-01", "mq-fdf-01"]


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


def test_secrets_are_readable_by_their_owner_only(tmp_path: Path) -> None:
    provision_broker(tmp_path, STATIONS)

    for name in ("ca.key", "server.key", "credentials.json", "passwords"):
        assert mode(tmp_path / name) == 0o600, name


def test_server_certificate_is_signed_by_the_ca_for_the_broker_names(tmp_path: Path) -> None:
    provision_broker(tmp_path, STATIONS)
    ca = x509.load_pem_x509_certificate((tmp_path / "ca.crt").read_bytes())
    server = x509.load_pem_x509_certificate((tmp_path / "server.crt").read_bytes())

    server.verify_directly_issued_by(ca)
    assert isinstance(server.public_key(), ec.EllipticCurvePublicKey)  # cheap on an ESP32
    names = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    assert set(names.get_values_for_type(x509.DNSName)) == {"broker", "localhost"}
    assert names.get_values_for_type(x509.IPAddress) == [ipaddress.ip_address("127.0.0.1")]
    assert ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca


def test_every_client_gets_a_distinct_random_password(tmp_path: Path) -> None:
    provision_broker(tmp_path, STATIONS)
    credentials = load_credentials(tmp_path / "credentials.json")

    assert set(credentials) == {INGESTOR_USER, *STATIONS}
    assert len(set(credentials.values())) == 3
    assert all(len(p) >= 32 for p in credentials.values())
    lines = (tmp_path / "passwords").read_text().splitlines()
    assert sorted(lines) == sorted(f"{u}:{p}" for u, p in credentials.items())


def test_rerunning_keeps_the_ca_and_existing_passwords(tmp_path: Path) -> None:
    provision_broker(tmp_path, STATIONS)
    ca_before = (tmp_path / "ca.crt").read_bytes()
    before = json.loads((tmp_path / "credentials.json").read_text())

    written = provision_broker(tmp_path, [*STATIONS, "gp-bte-01"])
    after = json.loads((tmp_path / "credentials.json").read_text())

    assert (tmp_path / "ca.crt").read_bytes() == ca_before
    assert "ca.crt" not in written
    assert {k: after[k] for k in before} == before  # devices keep working
    assert "gp-bte-01" in after


def test_acl_confines_stations_to_their_own_topics(tmp_path: Path) -> None:
    provision_broker(tmp_path, STATIONS)
    acl = (tmp_path / "acl").read_text()

    assert "pattern write antilles/%u/telemetry" in acl
    assert "pattern write antilles/%u/status" in acl
    assert "user ingestor\ntopic read antilles/+/telemetry" in acl
    assert "topic write" not in acl  # nobody may write arbitrary topics


def test_extra_hostnames_and_lan_addresses_are_in_the_certificate(tmp_path: Path) -> None:
    provision_broker(tmp_path, STATIONS, ["broker", "localhost", "mqtt.home", "192.168.1.10"])
    server = x509.load_pem_x509_certificate((tmp_path / "server.crt").read_bytes())
    names = server.extensions.get_extension_for_class(x509.SubjectAlternativeName).value

    assert "mqtt.home" in names.get_values_for_type(x509.DNSName)
    assert ipaddress.ip_address("192.168.1.10") in names.get_values_for_type(x509.IPAddress)
