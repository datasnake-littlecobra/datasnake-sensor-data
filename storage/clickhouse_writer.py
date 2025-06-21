import uuid
import logging
from datetime import datetime
from clickhouse_connect import get_client


class ClickHouseWriter:
    def __init__(
        self, host="127.0.0.1", database="datasnake", table="sensor_data_processed"
    ):
        self.host = host
        self.database = database
        self.table = table

        self.client = get_client(host=host, password="", database=database)
        logging.info("✅ ClickHouse connection established!")

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

    def write_to_clickhouse_batch(self, weather_data_processed_df):
        try:
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

            data = [
                (
                    str(uuid.uuid4()),
                    datetime.now(),
                    row.get("topic", "weather/data"),
                    row.get("device_id", None),
                    row.get("temp", 0.0),
                    row.get("humidity", 0.0),
                    row.get("pressure", 0.0),
                    row.get("lat", 0.0),
                    row.get("lon", 0.0),
                    row.get("alt", 0.0),
                    row.get("sats", 0),
                    row.get("wind_speed"),
                    row.get("wind_direction"),
                    "dummy_county",
                    row.get("city", "unknown"),
                    row.get("state", "unknown"),
                    row.get("country", "unknown"),
                    row.get("postal_code", "00000"),
                    ["00001", "00002"],
                    row.get("timestamp", datetime.utcnow()),
                )
                for row in weather_data_processed_df.to_dicts()
            ]

            self.client.insert(self.table, data, columns=columns)
            logging.info(f"ClickHouse: Inserted {len(data)} rows successfully.")

        except Exception as e:
            logging.error("Exception occurred writing to ClickHouse:", exc_info=e)
