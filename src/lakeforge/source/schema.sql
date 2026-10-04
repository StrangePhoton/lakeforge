-- Authoritative Milestone 0 source schema.
-- Docker Compose mounts this file into docker-entrypoint-initdb.d.
-- The Python loader executes this same file. Do not add a second copy.

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.vehicles (
    vehicle_id TEXT PRIMARY KEY,
    asset_type TEXT NOT NULL,
    model_code TEXT NOT NULL,
    site_code TEXT NOT NULL,
    status TEXT NOT NULL,
    commissioned_on DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS source.telemetry_events (
    event_id TEXT PRIMARY KEY,
    vehicle_id TEXT NOT NULL REFERENCES source.vehicles (vehicle_id),
    event_time TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed_kph DOUBLE PRECISION NOT NULL,
    engine_state TEXT NOT NULL,
    fuel_rate_lph DOUBLE PRECISION NOT NULL,
    engine_hours DOUBLE PRECISION NOT NULL
);
