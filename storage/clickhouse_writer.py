import uuid
import logging
from datetime import datetime
from clickhouse_connect import get_client


class ClickHouseWriter:
    def __init__(self, host="127.0.0.1", database="datasnake", table="sensor_data_processed"):
        self.host = host
        self.database = database
        self.table = table

        self.client = get_client(host=host, password="",  database=database)
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
            rows = []
            for row in weather_data_processed_df.to_dicts():
                rows.append(
                    {
                        "id": str(uuid.uuid4()),
                        "timestamp": datetime.now(),
                        "topic": row.get("topic", "weather/data"),
                        "device_id": row.get("device_id",None),
                        "temp": row.get("temp", 0.0),
                        "humidity": row.get("humidity", 0.0),
                        "pressure": row.get("pressure", 0.0),
                        "lat": row.get("lat", 0.0),
                        "lon": row.get("lon", 0.0),
                        "alt": row.get("alt", 0.0),
                        "sats": row.get("sats", 0),
                        "wind_speed": row.get("wind_speed",0.0),
                        "wind_direction": row.get("wind_direction",0.0),
                        "county": "dummy_county",
                        "city": row.get("city", None),
                        "state": row.get("state", None),
                        "country": row.get("country", None),
                        "postal_code": row.get("postal_code", None),
                        "nearby_postal_codes": ["00001", "00002"],
                        "processed_at": row.get("timestamp",datetime.utcnow()),
                    }
                )

            self.client.insert(self.table, rows)
            logging.info(f"ClickHouse: Inserted {len(rows)} rows successfully.")

        except Exception as e:
            logging.error("Exception occurred writing to ClickHouse:", exc_info=e)
