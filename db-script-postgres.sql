\connect datasnake;

-- Extensions (optional but safe)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================
-- RAW SENSOR DATA
-- =========================
CREATE TABLE IF NOT EXISTS sensor_data_raw (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    topic TEXT,
    device_id TEXT,

    temp REAL,
    humidity REAL,
    pressure REAL,

    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    alt REAL,

    sats SMALLINT,
    wind_speed REAL,
    wind_direction REAL,

    processed BOOLEAN DEFAULT FALSE,
    status TEXT DEFAULT 'new'
);

-- =========================
-- PROCESSED SENSOR DATA
-- =========================
CREATE TABLE IF NOT EXISTS sensor_data_processed (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    topic TEXT,
    device_id TEXT,

    temp REAL,
    humidity REAL,
    pressure REAL,

    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    alt REAL,

    sats SMALLINT,
    wind_speed REAL,
    wind_direction REAL,

    county TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    postal_code TEXT,

    nearby_postal_codes TEXT[],
    processed_at TIMESTAMPTZ NOT NULL
);


CREATE TABLE IF NOT EXISTS usps_postal_code_mapping (
    id BIGSERIAL PRIMARY KEY,

    postal_code TEXT NOT NULL,
    city TEXT NOT NULL,
    state TEXT NOT NULL,

    finance_number TEXT,
    facility_name TEXT,
    street_address TEXT,

    region TEXT,
    district TEXT,
    area_code TEXT,

    source_zip TEXT,
    employee_count INTEGER,

    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_usps_postal_code
    ON usps_postal_code_mapping (postal_code);
