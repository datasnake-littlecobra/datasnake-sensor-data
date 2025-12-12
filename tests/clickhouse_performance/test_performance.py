import polars as pl
import uuid
import random
import time
from datetime import datetime, timedelta
from clickhouse_connect import get_client


def generate_timeseries_weather_data(n_rows: int) -> pl.DataFrame:
    start_time = datetime.utcnow()
    zip_codes = [f"{random.randint(10000, 99999)}" for _ in range(100)]
    states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    counties = ["County_" + chr(65 + i) for i in range(10)]

    data = {
        "id": [str(uuid.uuid4()) for _ in range(n_rows)],
        "created_at": [start_time - timedelta(seconds=i) for i in range(n_rows)],
        "country": ["USA"] * n_rows,
        "state": [random.choice(states) for _ in range(n_rows)],
        "county": [random.choice(counties) for _ in range(n_rows)],
        "postal_code": [random.choice(zip_codes) for _ in range(n_rows)],
        "temp": [round(random.uniform(15.0, 35.0), 2) for _ in range(n_rows)],
        "wind_speed": [round(random.uniform(0.0, 25.0), 2) for _ in range(n_rows)],
    }
    return pl.DataFrame(data)


def setup_table(client):
    client.command(
        """
        CREATE TABLE IF NOT EXISTS datasnake.timeseries_weather (
            id UUID,
            created_at DateTime,
            country String,
            state String,
            county String,
            postal_code String,
            temp Float32,
            wind_speed Float32
        ) ENGINE = MergeTree
        PARTITION BY toYYYYMM(created_at)
        ORDER BY (postal_code, created_at)
        """
    )
    print("✅ Table ready.")


def insert_and_query(n_rows: int):
    print(f"\n🚀 Benchmarking {n_rows:,} rows")
    df = generate_timeseries_weather_data(n_rows)

    client = get_client(host="127.0.0.1", database="datasnake")
    setup_table(client)

    # Convert to tuples and insert
    columns = [
        "id",
        "created_at",
        "country",
        "state",
        "county",
        "postal_code",
        "temp",
        "wind_speed",
    ]
    tuples = [tuple(row) for row in df.iter_rows()]

    # Insert
    start = time.time()
    client.insert("timeseries_weather", tuples, column_names=columns)
    duration = time.time() - start
    print(f"📝 Inserted {n_rows:,} rows in {duration:.2f}s")

    # Query
    start = time.time()
    result = client.query(
        """
        SELECT postal_code, AVG(temp) 
        FROM datasnake.timeseries_weather 
        GROUP BY postal_code 
        ORDER BY AVG(temp) DESC 
        LIMIT 10
        """
    )
    duration = time.time() - start
    print(f"📊 Query returned {len(result.result_rows)} rows in {duration:.2f}s")
    print("Top rows:", result.result_rows)


# Try with 10K first, scale up to 1M+
if __name__ == "__main__":
    insert_and_query(10000)
    # insert_and_query(1_000_000)
    # insert_and_query(10_000_000)
