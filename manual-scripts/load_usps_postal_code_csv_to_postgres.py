import csv
import logging
import psycopg
from pathlib import Path

logging.basicConfig(level=logging.INFO)

CSV_PATH = Path("/path/to/usps.csv")
POSTGRES_DSN = "..."  # your DSN

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
);
"""


def main():
    conn = psycopg.connect(POSTGRES_DSN)
    conn.autocommit = True

    inserted = 0
    skipped = 0

    with conn.cursor() as cur, CSV_PATH.open(
        "r", encoding="utf-8-sig", newline=""
    ) as f:
        reader = csv.reader(f, delimiter=",")  # ✅ FIX: comma

        for i, row in enumerate(reader):
            if i == 0 and row and row[0].strip().upper() == "AREA NAME":
                logging.info("Skipping header row")
                continue

            try:
                if len(row) < 11:
                    raise ValueError(f"expected 11 cols, got {len(row)}")

                record = {
                    "area_name": row[0].strip(),
                    "area_code": row[1].strip(),
                    "district_name": row[2].strip(),
                    "district_number": row[3].strip(),
                    "postal_code": row[4].strip(),
                    "locale_name": row[5].strip(),
                    "address": row[6].strip(),
                    "city": row[7].strip(),
                    "state": row[8].strip(),
                    "zip_code": row[9].strip(),
                    "zip_code_4": row[10].strip(),
                }

                cur.execute(INSERT_SQL, record)
                inserted += 1

                if inserted % 5000 == 0:
                    logging.info(f"Inserted {inserted} rows...")

            except Exception as e:
                skipped += 1
                logging.warning(f"Skipping bad row #{i}: {row} | error={e}")

    logging.info(f"✅ Done. Inserted={inserted}, Skipped={skipped}")


if __name__ == "__main__":
    main()
