import csv
import psycopg
from pathlib import Path
import os
CSV_PATH = Path("/home/dev/datasnake-sensor-data/public-datasets/usps_postal_code_mapping.csv")
POSTGRES_DSN = os.getenv("POSTGRES_DSN")

INSERT_SQL = """
INSERT INTO usps_postal_code_mapping (
    postal_code,
    city,
    state,
    finance_number,
    facility_name,
    street_address,
    region,
    district,
    area_code,
    source_zip,
    employee_count
)
VALUES (
    %(postal_code)s,
    %(city)s,
    %(state)s,
    %(finance_number)s,
    %(facility_name)s,
    %(street_address)s,
    %(region)s,
    %(district)s,
    %(area_code)s,
    %(source_zip)s,
    %(employee_count)s
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
                record = {
                    "region": row[0],
                    "district": row[1],
                    "area_code": row[2],
                    "finance_number": row[3],
                    "postal_code": row[4],
                    "facility_name": row[5],
                    "street_address": row[6],
                    "city": row[7],
                    "state": row[8],
                    "source_zip": row[9],
                    "employee_count": int(row[10]) if row[10].isdigit() else None,
                }

                cur.execute(INSERT_SQL, record)

            except Exception as e:
                print(f"⚠️ Skipping bad row: {row} | error={e}")

    print("✅ USPS postal code data loaded")


if __name__ == "__main__":
    main()
