import uuid
import logging
from datetime import datetime
from dateutil.parser import parse as parse_date

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
