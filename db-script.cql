CREATE KEYSPACE IF NOT EXISTS datasnake WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE datasnake;
DROP TABLE sensor_data_raw;

CREATE TABLE sensor_data_raw (
    id UUID PRIMARY KEY,
    timestamp TIMESTAMP,
    topic TEXT,
    device_id TEXT,
    temp FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    lat FLOAT,
    lon FLOAT,
    alt FLOAT,
    sats INT,
    wind_speed FLOAT,
    wind_direction FLOAT,
    processed BOOLEAN,
    status TEXT DEFAULT 'new'   -- Track processing state: 'new', 'processed'
);

CREATE TABLE sensor_data_processed (
    id UUID,
    timestamp TIMESTAMP,
    topic TEXT,
    device_id TEXT,
    temp FLOAT,
    humidity FLOAT,
    pressure FLOAT,
    lat FLOAT,
    lon FLOAT,
    alt FLOAT,
    sats INT,
    wind_speed FLOAT,
    wind_direction FLOAT,
    county TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    postal_code TEXT, 
    nearby_postal_codes LIST<TEXT>, 
    processed_at TIMESTAMP,
    PRIMARY KEY ((country, state), postal_code, timestamp, id)
) WITH CLUSTERING ORDER BY (postal_code ASC, timestamp DESC);
