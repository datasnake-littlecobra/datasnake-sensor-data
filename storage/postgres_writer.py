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

    def write_raw_batch(self, df: pl.DataFrame):
        """
        Writes raw sensor events to public.sensor_data_raw.
        Expects df columns similar to your generator/consumer payload:
        - topic (optional)
        - device_id
        - temp, humidity, pressure
        - lat, lon, alt
        - sats, wind_speed, wind_direction
        - timestamp (string or datetime, optional)
        """
        if df.is_empty():
            return

        rows = []

        # Quick DB context check (optional but helpful in early phases)
        with self.conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            logging.warning(f"RAW WRITER DB CONTEXT: {cur.fetchone()}")

        for row in df.to_dicts():
            event_ts = row.get("timestamp")

            if isinstance(event_ts, str):
                event_ts = parse_date(event_ts)
            elif event_ts is None:
                event_ts = datetime.utcnow()

            rows.append(
                (
                    str(uuid.uuid4()),
                    event_ts,  # timestamp in table = event timestamp
                    row.get("topic", "weather/data") or "weather/data",
                    row.get("device_id") or "",
                    float(row.get("temp") or 0.0),
                    float(row.get("humidity") or 0.0),
                    float(row.get("pressure") or 0.0),
                    float(row.get("lat") or 0.0),
                    float(row.get("lon") or 0.0),
                    float(row.get("alt") or 0.0),
                    int(row.get("sats") or 0),
                    float(row.get("wind_speed") or 0.0),
                    float(row.get("wind_direction") or 0.0),
                    False,  # processed
                    "new",  # status
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
                processed,
                status
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s
            )
        """

        try:
            with self.conn.cursor() as cur:
                cur.executemany(sql, rows)

                # Verify immediately (table name here is explicit)
                cur.execute("SELECT COUNT(*) FROM public.sensor_data_raw")
                logging.warning(
                    f"RAW POST-INSERT COUNT (same conn): {cur.fetchone()[0]}"
                )

            logging.info(
                f"✅ PostgreSQL RAW: inserted {len(rows)} rows into {self.table}"
            )

        except Exception:
            logging.exception("❌ PostgreSQL RAW INSERT failed")
            raise

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


def write_ground_enriched(self, df: pl.DataFrame):
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
            cur.executemany(sql, rows)

        logging.info(f"✅ PostgreSQL ENRICHED: inserted {len(rows)} rows")

    except Exception:
        logging.exception("❌ PostgreSQL ENRICHED INSERT failed")
        raise
