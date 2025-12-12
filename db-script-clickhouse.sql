-- Create database if not exists
CREATE DATABASE IF NOT EXISTS datasnake;

-- Use the database
USE datasnake;

-- Create sensor_data_raw table if not exists
CREATE TABLE IF NOT EXISTS sensor_data_raw (
    id UUID,
    timestamp DateTime,
    topic String,
    device_id String,
    temp Float32,
    humidity Float32,
    pressure Float32,
    lat Float32,
    lon Float32,
    alt Float32,
    sats UInt8,
    wind_speed Float32,
    wind_direction Float32,
    processed UInt8 DEFAULT 0,
    status String DEFAULT 'new'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, device_id)
TTL timestamp + INTERVAL 30 DAY;

-- Create sensor_data_processed table if not exists
CREATE TABLE IF NOT EXISTS sensor_data_processed (
    id UUID,
    timestamp DateTime,
    topic String,
    device_id String,
    temp Float32,
    humidity Float32,
    pressure Float32,
    lat Float32,
    lon Float32,
    alt Float32,
    sats UInt8,
    wind_speed Float32,
    wind_direction Float32,
    county String,
    city String,
    state String,
    country String,
    postal_code String,
    nearby_postal_codes Array(String),
    processed_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (country, state, postal_code, timestamp)
TTL timestamp + INTERVAL 90 DAY;
