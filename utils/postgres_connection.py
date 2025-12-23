import logging
import psycopg
import os
from typing import Optional


class PostgresConnection:
    """
    Lightweight Postgres connection provider.

    - Lazily creates a connection
    - Reuses the same connection per process
    - Keeps timezone consistent
    """

    def __init__(self, dsn: Optional[str] = None):
        self.dsn = dsn or os.getenv("DATABASE_DSN")
        if not self.dsn:
            raise RuntimeError("DATABASE_DSN is not set")

        self._conn = None

    def get_conn(self):
        if self._conn is None or self._conn.closed:
            logging.info("🔌 Creating new PostgreSQL connection")
            self._conn = psycopg.connect(self.dsn)
            self._conn.autocommit = True

            with self._conn.cursor() as cur:
                cur.execute("SET timezone = 'UTC'")

        return self._conn

    def close(self):
        if self._conn and not self._conn.closed:
            logging.info("🔌 Closing PostgreSQL connection")
            self._conn.close()
