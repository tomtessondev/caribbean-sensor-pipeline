-- The ingestor writes with its own role: it can add readings, rejects and status changes,
-- and nothing else. It cannot delete or rewrite telemetry, read the flags, or touch the
-- schema. Its login and password are set by `sensor-pipeline migrate --ingestor-password`.
do $$
begin
    if not exists (select from pg_roles where rolname = 'sensor_ingestor') then
        create role sensor_ingestor nologin;
    end if;
end
$$;

grant usage on schema public to sensor_ingestor;
-- Duplicates are skipped, never overwritten. The insert says ON CONFLICT DO NOTHING without
-- naming the key: naming it would also require SELECT, and a column-level grant does not
-- survive TimescaleDB's compressed internal table. INSERT alone is enough.
grant insert on telemetry, ingest_errors to sensor_ingestor;
grant usage on sequence ingest_errors_id_seq to sensor_ingestor;
-- Status is the one row the ingestor updates in place (the last known state).
grant select, insert, update (status, changed_at) on station_status to sensor_ingestor;
