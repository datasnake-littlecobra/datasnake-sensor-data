import uuid
import logging
from datetime import datetime
from io import StringIO
from dateutil.parser import parse as parse_date

import psycopg
import polars as pl


class PostgresWriter:
    def __init__(self, dsn: str, table: str):
        self.dsn = dsn
        self.table = table
        self.conn = psycopg.connect(dsn)
        self.conn.execute("SET timezone = 'UTC'")
        logging.info("PostgreSQL connection established")


def write_batch(self, df: pl.DataFrame):
    if df.is_empty():
        return

    buffer = StringIO()

    for row in df.to_dicts():
        processed_ts = row.get("timestamp")

        if isinstance(processed_ts, str):
            processed_ts = parse_date(processed_ts)
        elif processed_ts is None:
            processed_ts = datetime.utcnow()

        buffer.write(
            "\t".join(
                map(
                    str,
                    [
                        uuid.uuid4(),
                        datetime.utcnow().isoformat(),
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
                        "{00001,00002}",
                        processed_ts.isoformat(),
                    ],
                )
            )
            + "\n"
        )

    buffer.seek(0)

    try:
        with self.conn.transaction():
            with self.conn.cursor() as cur:
                cur.copy(
                    f"""
                    COPY {self.table} (
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
                    FROM STDIN
                    """,
                    buffer,
                )

        logging.info(f"✅ PostgreSQL: inserted {df.height} rows")

    except Exception as e:
        logging.error("❌ PostgreSQL COPY failed", exc_info=e)
        raise
