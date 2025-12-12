import argparse
import logging
from pathlib import Path

import polars as pl

from storage.clickhouse_writer import ClickHouseWriter


def ensure_columns(df: pl.DataFrame, expected_cols):
	"""Add missing columns with None values so writer can safely read keys."""
	existing = set(df.columns)
	for c in expected_cols:
		if c not in existing:
			df = df.with_columns(pl.lit(None).alias(c))
	return df


def main():
	parser = argparse.ArgumentParser(
		description="Move CSV data to ClickHouse using ClickHouseWriter"
	)
	parser.add_argument("--csv", required=True, help="Path to input CSV file")
	parser.add_argument("--host", default="127.0.0.1", help="ClickHouse host")
	parser.add_argument("--database", default="", help="ClickHouse database")
	parser.add_argument(
		"--table", default="", help="ClickHouse table name"
	)
	parser.add_argument(
		"--dry-run",
		action="store_true",
		help="Read and show DataFrame info but do not write to ClickHouse",
	)

	args = parser.parse_args()

	logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

	csv_path = Path(args.csv)
	if not csv_path.exists():
		logging.error("CSV file not found: %s", csv_path)
		raise SystemExit(1)

	logging.info("Reading CSV: %s", csv_path)
	try:
		df = pl.read_csv(str(csv_path))
	except Exception as e:
		logging.exception("Failed to read CSV: %s", e)
		raise

	logging.info("CSV loaded: %d rows, %d cols", df.height, df.width)

	# expected keys used by ClickHouseWriter.write_to_clickhouse_batch
	expected = [
		"topic",
		"device_id",
		"temp",
		"humidity",
		"pressure",
		"lat",
		"lon",
		"alt",
		"sats",
		"wind_speed",
		"wind_direction",
		"county",
		"city",
		"state",
		"country",
		"postal_code",
		"timestamp",
	]

	df = ensure_columns(df, expected)

	# Optional: normalize column names to lower-case
	df = df.rename({c: c.lower() for c in df.columns})

	if args.dry_run:
		logging.info("Dry-run: showing first 5 rows")
		print(df.head(5))
		logging.info("Dry-run complete. Not writing to ClickHouse.")
		return

	# instantiate writer and write
	writer = ClickHouseWriter(host=args.host, database=args.database, table=args.table)
	logging.info("Writing to ClickHouse table %s.%s", args.database, args.table)
	writer.write_to_clickhouse_batch(df)

	logging.info("Done.")


if __name__ == "__main__":
	main()

