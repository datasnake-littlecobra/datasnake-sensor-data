import csv
import psycopg
from pathlib import Path
import os
import logging

CSV_PATH = Path("/home/dev/datasnake-sensor-data/public-datasets/usps_postal_code_mapping.csv")
POSTGRES_DSN = os.getenv("POSTGRES_DSN")

INSERT_SQL = """
INSERT INTO usps_postal_code_mapping (
    postal_code,
    area_name,
    area_code,
    district_name,
    district_number,
    locale_name,
    address,
    city,
    state,
    zip_code,
    zip_code_4
)
VALUES (
    %(postal_code)s,
    %(area_name)s,
    %(area_code)s,
    %(district_name)s,
    %(district_number)s,
    %(locale_name)s,
    %(address)s,
    %(city)s,
    %(state)s,
    %(zip_code)s,
    %(zip_code_4)s
)
ON CONFLICT DO NOTHING;
"""


def main():
    conn = psycopg.connect(POSTGRES_DSN)
    conn.autocommit = True

    with conn.cursor() as cur, CSV_PATH.open() as f:
        reader = csv.reader(f, delimiter="\t")

        for row in reader:
            try:
                logging.info(f"Processing row: {row}")
                record = {
                    "area_name": row[0],
                    "area_code": row[1],
                    "district_name": row[2],
                    "district_number": row[3],
                    "postal_code": row[4],
                    "locale_name": row[5],
                    "address": row[6],
                    "city": row[7],
                    "state": row[8],
                    "zip_code": row[9],
                    "zip_code_4": row[10],
                }

                cur.execute(INSERT_SQL, record)

            except Exception as e:
                print(f"⚠️ Skipping bad row: {row} | error={e}")

    print("✅ USPS postal code data loaded")


if __name__ == "__main__":
    main()
