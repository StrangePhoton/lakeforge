-- Authoritative operational source schema.
-- Docker Compose mounts this file into docker-entrypoint-initdb.d.
-- The Python loader executes this same file. Do not add a second copy.
--
-- Telemetry is an externally produced machine event feed. It is not a child
-- of the vehicle master. Readings may arrive before master-data reconciliation
-- or with an equipment identifier the source has not resolved, so vehicle_id
-- is not a foreign key and event_time may be absent. received_at is mandatory.
-- Sites, vehicles, alarms, and work orders stay relationally constrained.

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.sites (
    site_id TEXT PRIMARY KEY,
    site_name TEXT NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    timezone_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS source.vehicles (
    vehicle_id TEXT PRIMARY KEY,
    asset_type TEXT NOT NULL,
    model_code TEXT NOT NULL,
    site_id TEXT NOT NULL REFERENCES source.sites (site_id),
    status TEXT NOT NULL,
    commissioned_on DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS source.telemetry_events (
    event_id TEXT PRIMARY KEY,
    vehicle_id TEXT NULL,
    event_time TIMESTAMPTZ NULL,
    received_at TIMESTAMPTZ NOT NULL,
    latitude DOUBLE PRECISION NULL,
    longitude DOUBLE PRECISION NULL,
    speed_kph DOUBLE PRECISION NULL,
    engine_state TEXT NULL,
    fuel_rate_lph DOUBLE PRECISION NULL,
    engine_hours DOUBLE PRECISION NULL
);

CREATE TABLE IF NOT EXISTS source.alarms (
    alarm_id TEXT PRIMARY KEY,
    vehicle_id TEXT NOT NULL REFERENCES source.vehicles (vehicle_id),
    event_id TEXT NULL,
    alarm_code TEXT NOT NULL,
    severity TEXT NOT NULL,
    raised_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS source.work_orders (
    work_order_id TEXT PRIMARY KEY,
    vehicle_id TEXT NOT NULL REFERENCES source.vehicles (vehicle_id),
    site_id TEXT NOT NULL REFERENCES source.sites (site_id),
    alarm_id TEXT NULL REFERENCES source.alarms (alarm_id),
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ NULL,
    work_type TEXT NOT NULL,
    cause_code TEXT NOT NULL,
    status TEXT NOT NULL
);
