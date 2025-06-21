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
        "zip_code": [random.choice(zip_codes) for _ in range(n_rows)],
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
        zip_code String,
        temp Float32,
        wind_speed Float32
    ) ENGINE = MergeTree
    PARTITION BY toYYYYMM(created_at)
    ORDER BY (zip_code, created_at)
    """
    )
    print("✅ Table ready.")


def insert_and_query(n_rows: int):
    print(f"\n🚀 Benchmarking {n_rows:,} rows")
    df = generate_timeseries_weather_data(n_rows)

    client = get_client(host="127.0.0.1", database="datasnake")
    setup_table(client)

    # Insert
    start = time.time()
    client.insert("timeseries_weather", df.to_dicts())
    duration = time.time() - start
    print(f"📝 Inserted {n_rows:,} rows in {duration:.2f}s")

    # Query
    start = time.time()
    result = client.query(
        """
        SELECT zip_code, AVG(temp) 
        FROM datasnake.timeseries_weather 
        GROUP BY zip_code 
        ORDER BY AVG(temp) DESC 
        LIMIT 10
    """
    )
    duration = time.time() - start
    print(f"📊 Query returned {len(result.result_rows)} rows in {duration:.2f}s")
    print("Top rows:", result.result_rows)


# Try with 1M first
if __name__ == "__main__":
    insert_and_query(1_000_000)
    # You can then scale to 10M, 50M, 100M one-by-one depending on memory
