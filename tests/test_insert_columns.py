import polars as pl
import uuid
import logging
from datetime import datetime
import random
from clickhouse_connect import get_client

# Configure logging
logging.basicConfig(level=logging.INFO)


# Step 1: Generate dummy data
def generate_dummy_data(n=1000):
    return pl.DataFrame(
        {
            "device_id": [f"dev_{i % 10}" for i in range(n)],
            "lat": [random.uniform(30.0, 50.0) for _ in range(n)],
            "lon": [random.uniform(-120.0, -70.0) for _ in range(n)],
            "temp": [random.uniform(10, 40) for _ in range(n)],
            "humidity": [random.uniform(20, 80) for _ in range(n)],
            "pressure": [random.uniform(950, 1050) for _ in range(n)],
            "alt": [random.uniform(10, 100) for _ in range(n)],
            "sats": [random.randint(5, 15) for _ in range(n)],
            "wind_speed": [random.uniform(0, 20) for _ in range(n)],
            "wind_direction": [random.uniform(0, 360) for _ in range(n)],
            "city": ["CityX"] * n,
            "state": ["StateY"] * n,
            "country": ["USA"] * n,
            "postal_code": ["12345"] * n,
            "timestamp": [datetime.utcnow() for _ in range(n)],
            "topic": ["weather/data"] * n,
        }
    )


# Step 2: Insert data into ClickHouse
def insert_into_clickhouse(
    df, host="127.0.0.1", database="datasnake", table="sensor_data_processed"
):
    try:
        client = get_client(host=host, database=database)
        logging.info("Connected to ClickHouse")

        columns = [
            "id",
            "timestamp",
            "topic",
            "device_id",
            "temp",
            "humidity",
            "pressure",
            "lat",
            "lon",
            "alt",
            "sats",
            "wind_speed",
            "wind_direction",
            "county",
            "city",
            "state",
            "country",
            "postal_code",
            "nearby_postal_codes",
            "processed_at",
        ]

        rows = []
        for row in df.to_dicts():
            rows.append(
                (
                    str(uuid.uuid4()),
                    datetime.utcnow(),
                    row["topic"],
                    row["device_id"],
                    row["temp"],
                    row["humidity"],
                    row["pressure"],
                    row["lat"],
                    row["lon"],
                    row["alt"],
                    row["sats"],
                    row["wind_speed"],
                    row["wind_direction"],
                    "dummy_county",
                    row["city"],
                    row["state"],
                    row["country"],
                    row["postal_code"],
                    ["00001", "00002"],
                    row["timestamp"],
                )
            )

        client.insert(table, rows, column_names=columns)
        logging.info(f"✅ Inserted {len(rows)} rows into {database}.{table}")

    except Exception as e:
        logging.error("❌ Failed inserting into ClickHouse:", exc_info=e)


# Step 3: Run it
if __name__ == "__main__":
    logging.info("🚀 Generating data...")
    df = generate_dummy_data(n=5000)  # Change n to test large volumes
    insert_into_clickhouse(df)
