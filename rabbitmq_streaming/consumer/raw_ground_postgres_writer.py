import uuid
import logging
from datetime import datetime
from dateutil.parser import parse as parse_date
import polars as pl

import psycopg


class RawPostgresWriter:
    def __init__(self, dsn: str):
        self.conn = psycopg.connect(dsn)
        self.conn.autocommit = True
        self.conn.execute("SET timezone = 'UTC'")
        logging.info("RAW PostgreSQL connection established")

    def write_event(self, event: dict):
        """
        Writes ONE raw sensor event.
        Called per message from RabbitMQ.
        """

        event_ts = event.get("timestamp")
        if isinstance(event_ts, str):
            event_ts = parse_date(event_ts)
        elif event_ts is None:
            event_ts = datetime.utcnow()

        sql = """
            INSERT INTO public.sensor_data_raw (
                id,
                timestamp,
                topic,
                device_id,
                temp,
                humidity,
                pressure,
                lat,
                lon,
                alt,
                sats,
                wind_speed,
                wind_direction,
                processed,
                status
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """

        values = (
            str(uuid.uuid4()),
            event_ts,
            event.get("topic", "weather/data"),
            event.get("device_id", ""),
            event.get("temp", 0.0),
            event.get("humidity", 0.0),
            event.get("pressure", 0.0),
            event.get("lat", 0.0),
            event.get("lon", 0.0),
            event.get("alt", 0.0),
            event.get("sats", 0),
            event.get("wind_speed", 0.0),
            event.get("wind_direction", 0.0),
            False,
            "new",
        )

        try:
            with self.conn.cursor() as cur:
                cur.execute(sql, values)

        except Exception:
            logging.exception("❌ RAW PostgreSQL insert failed")
            raise

    def write_ground_enriched(
        self, df: pl.DataFrame, table: str = "sensor_data_processed"
    ):
        """
        Writes enriched ground sensor events to public.sensor_data_processed.
        Designed for streaming / RabbitMQ consumers (small batches).
        """
        if df.is_empty():
            return

        rows = []

        with self.conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            logging.debug(f"ENRICHED WRITER DB CONTEXT: {cur.fetchone()}")

        for row in df.to_dicts():
            event_ts = row.get("timestamp")
            if isinstance(event_ts, str):
                event_ts = parse_date(event_ts)
            elif event_ts is None:
                event_ts = datetime.utcnow()

            rows.append(
                (
                    str(uuid.uuid4()),
                    event_ts,
                    row.get("topic", "weather/data"),
                    row.get("device_id"),
                    float(row.get("temp") or 0.0),
                    float(row.get("humidity") or 0.0),
                    float(row.get("pressure") or 0.0),
                    float(row.get("lat") or 0.0),
                    float(row.get("lon") or 0.0),
                    float(row.get("alt") or 0.0),
                    int(row.get("sats") or 0),
                    float(row.get("wind_speed") or 0.0),
                    float(row.get("wind_direction") or 0.0),
                    row.get("county"),
                    row.get("city"),
                    row.get("state"),
                    row.get("country"),
                    row.get("postal_code"),
                    None,  # nearby_postal_codes (defer)
                    datetime.utcnow(),  # processed_at
                )
            )

        sql = f"""
            INSERT INTO public.{table} (
                id,
                timestamp,
                topic,
                device_id,
                temp,
                humidity,
                pressure,
                lat,
                lon,
                alt,
                sats,
                wind_speed,
                wind_direction,
                usps_locale_name,
                county,
                city,
                state,
                country,
                postal_code,
                nearby_postal_codes,
                processed_at
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s
            )
        """

        try:
            with self.conn.cursor() as cur:
                cur.executemany(sql, rows)

            logging.info(f"✅ PostgreSQL ENRICHED: inserted {len(rows)} rows")

        except Exception:
            logging.exception("❌ PostgreSQL ENRICHED INSERT failed")
            raise
