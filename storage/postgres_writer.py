import uuid
import logging
from datetime import datetime
from dateutil.parser import parse as parse_date

import psycopg
import polars as pl


class PostgresWriter:
    def __init__(self, dsn: str, table: str):
        self.dsn = dsn
        self.table = table
        self.conn = psycopg.connect(dsn)
        self.conn.autocommit = True
        self.conn.execute("SET timezone = 'UTC'")
        logging.info("PostgreSQL connection established")

    def write_batch(self, df: pl.DataFrame):
        if df.is_empty():
            return

        rows = []
        logging.info("checking the postgres db context")
        with self.conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            logging.warning(f"WRITER DB CONTEXT: {cur.fetchone()}")
        # return
        for row in df.to_dicts():
            processed_ts = row.get("timestamp")

            if isinstance(processed_ts, str):
                processed_ts = parse_date(processed_ts)
            elif processed_ts is None:
                processed_ts = datetime.utcnow()

            rows.append(
                (
                    str(uuid.uuid4()),
                    datetime.utcnow(),
                    row.get("topic", "weather/data") or "weather/data",
                    row.get("device_id") or "",
                    row.get("temp") or 0.0,
                    row.get("humidity") or 0.0,
                    row.get("pressure") or 0.0,
                    row.get("lat") or 0.0,
                    row.get("lon") or 0.0,
                    row.get("alt") or 0.0,
                    row.get("sats") or 0,
                    row.get("wind_speed") or 0.0,
                    row.get("wind_direction") or 0.0,
                    "dummy_county",
                    row.get("city") or "unknown",
                    row.get("state") or "unknown",
                    row.get("country") or "unknown",
                    row.get("postal_code") or "00000",
                    ["00001", "00002"],
                    processed_ts,
                )
            )

        sql = f"""
            INSERT INTO public.{self.table} (
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
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """

        try:
            with self.conn.cursor() as cur:
                # ACTUAL INSERT
                cur.executemany(sql, rows)

                # VERIFY immediately
                cur.execute("SELECT COUNT(*) FROM public.sensor_data_processed")
                logging.warning(f"POST-INSERT COUNT (same conn): {cur.fetchone()[0]}")

            logging.info(f"✅ PostgreSQL: inserted {len(rows)} rows")

        except Exception:
            logging.exception("❌ PostgreSQL INSERT failed")
            raise

        except Exception:
            logging.exception("❌ PostgreSQL INSERT failed")
            raise
