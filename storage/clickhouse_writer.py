import uuid
import logging
from datetime import datetime
from clickhouse_connect import get_client
from dateutil.parser import parse as parse_date


class ClickHouseWriter:
    def __init__(
        self, host="127.0.0.1", database="datasnake", table="sensor_data_processed"
    ):
        self.host = host
        self.database = database
        self.table = table

        self.client = get_client(host=host, password="", database=database)
        logging.info("ClickHouse connection established!")

        # Ensure table exists (you can manage this outside if needed)
        # self._ensure_table()

    # def _ensure_table(self):
    #     create_query = f"""
    #     CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
    #         id UUID,
    #         timestamp DateTime,
    #         topic String,
    #         temp Float32,
    #         humidity Float32,
    #         pressure Float32,
    #         lat Float32,
    #         lon Float32,
    #         alt Float32,
    #         sats UInt8,
    #         county String,
    #         city String,
    #         state String,
    #         country String,
    #         postal_code String,
    #         nearby_postal_codes Array(String),
    #         processed_at DateTime
    #     ) ENGINE = MergeTree
    #     PARTITION BY toYYYYMM(timestamp)
    #     ORDER BY (device_id, timestamp)
    #     """
    #     self.client.command(create_query)

    def write_to_clickhouse_batch_old(self, weather_data_processed_df):
        try:
            rows = []
            for row in weather_data_processed_df.to_dicts():
                rows.append(
                    {
                        "id": str(uuid.uuid4()),
                        "timestamp": datetime.now(),
                        "topic": row.get("topic", "weather/data") or "weather/data",
                        "device_id": row.get("device_id") or "",
                        "temp": row.get("temp") if row.get("temp") is not None else 0.0,
                        "humidity": (
                            row.get("humidity")
                            if row.get("humidity") is not None
                            else 0.0
                        ),
                        "pressure": (
                            row.get("pressure")
                            if row.get("pressure") is not None
                            else 0.0
                        ),
                        "lat": row.get("lat") if row.get("lat") is not None else 0.0,
                        "lon": row.get("lon") if row.get("lon") is not None else 0.0,
                        "alt": row.get("alt") if row.get("alt") is not None else 0.0,
                        "sats": row.get("sats") if row.get("sats") is not None else 0,
                        "wind_speed": (
                            row.get("wind_speed")
                            if row.get("wind_speed") is not None
                            else 0.0
                        ),
                        "wind_direction": (
                            row.get("wind_direction")
                            if row.get("wind_direction") is not None
                            else 0.0
                        ),
                        "county": "dummy_county",
                        "city": row.get("city") or "unknown",
                        "state": row.get("state") or "unknown",
                        "country": row.get("country") or "unknown",
                        "postal_code": row.get("postal_code") or "00000",
                        "nearby_postal_codes": [
                            "00001",
                            "00002",
                        ],  # Make sure your schema supports Array(String)
                        "processed_at": row.get("timestamp") or datetime.utcnow(),
                    }
                )

            self.client.insert(self.table, rows)
            logging.info(f"ClickHouse: Inserted {len(rows)} rows successfully.")

        except Exception as e:
            logging.error("Exception occurred writing to ClickHouse:", exc_info=e)

    def write_to_clickhouse_batch(self, weather_data_processed_df):
        try:
            # Prepare insert columns in the correct order
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

            # Transform Polars rows to list of tuples for ClickHouse
            rows = []
            for row in weather_data_processed_df.to_dicts():
                rows.append(
                    (
                        str(uuid.uuid4()),
                        datetime.utcnow(),
                        row.get("topic", "weather/data") or "weather/data",
                        row.get("device_id") or "",
                        row.get("temp") if row.get("temp") is not None else 0.0,
                        row.get("humidity") if row.get("humidity") is not None else 0.0,
                        row.get("pressure") if row.get("pressure") is not None else 0.0,
                        row.get("lat") if row.get("lat") is not None else 0.0,
                        row.get("lon") if row.get("lon") is not None else 0.0,
                        row.get("alt") if row.get("alt") is not None else 0.0,
                        row.get("sats") if row.get("sats") is not None else 0,
                        (
                            row.get("wind_speed")
                            if row.get("wind_speed") is not None
                            else 0.0
                        ),
                        (
                            row.get("wind_direction")
                            if row.get("wind_direction") is not None
                            else 0.0
                        ),
                        "dummy_county",  # or row.get("county", "unknown") if present
                        row.get("city") or "unknown",
                        row.get("state") or "unknown",
                        row.get("country") or "unknown",
                        row.get("postal_code") or "00000",
                        ["00001", "00002"],  # Array(String)
                        (
                            parse_date(row.get("timestamp"))
                            if isinstance(row.get("timestamp"), str)
                            else row.get("timestamp") or datetime.utcnow()
                        ),
                    )
                )

            # Insert using ClickHouse Connect
            self.client.insert(self.table, rows, column_names=columns)
            logging.info(f"✅ ClickHouse: Inserted {len(rows)} rows successfully.")

        except Exception as e:
            logging.error("❌ Exception occurred writing to ClickHouse:", exc_info=e)
