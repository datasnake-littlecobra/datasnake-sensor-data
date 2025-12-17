-- Create database if not exists
CREATE DATABASE datasnake;

CREATE TABLE sensor_data_raw
(
    id UUID NOT NULL,
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
    status TEXT DEFAULT 'new',

    PRIMARY KEY (id)
);


CREATE TABLE sensor_data_processed
(
    id UUID NOT NULL,
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

    nearby_postal_codes TEXT
    [],

    processed_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY
    (id)
);

-- CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ALTER TABLE sensor_data_raw
-- ALTER COLUMN id SET DEFAULT gen_random_uuid();

-- ALTER TABLE sensor_data_processed
-- ALTER COLUMN id SET DEFAULT gen_random_uuid();
