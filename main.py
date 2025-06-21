import polars as pl
from datetime import datetime
from reader.log_reader import LogReader
from storage.delta_writer import DeltaWriter
from storage.clickhouse_writer import ClickHouseWriter
from geoprocessor.search_locations import WeatherDataLocationSearcher
import logging

LOG_FILE = "/home/dev/mqtt-python/mqtt_weather_logs.log"
PROCESSED_SENSOR_DATA_DELTA_PATH = "/datasnake-deltalake-sensor-data-processed"
WOF_DELTA_PATH = "/home/resources/deltalake-wof-oregon"
BATCH_SIZE = 5000


def process_batch(batch_df, searcher, delta_writer, clickhouse_writer, start, end):
    logging.info(f"🚀 Processing batch rows {start} to {end}")

    # Enrich
    t1 = datetime.now()
    enriched_df = searcher.enrich_weather_data_optimized(batch_df)
    enriched_df = enriched_df.with_columns(
        pl.col("city").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("state").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("country").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("postal_code").cast(pl.Utf8).fill_null("00000"),
    )
    t2 = datetime.now()
    logging.info(f"🗺️  Enrichment time: {t2 - t1}")

    # Delta write
    t1 = datetime.now()
    delta_writer.write_to_deltalake(enriched_df)
    t2 = datetime.now()
    logging.info(f"📦 DeltaLake write time: {t2 - t1}")

    # ClickHouse insert
    t1 = datetime.now()
    clickhouse_writer.write_to_clickhouse_batch(enriched_df)
    t2 = datetime.now()
    logging.info(f"⚡ ClickHouse write time: {t2 - t1}")


def main():
    logging.info("📥 Reading weather data log...")
    weather_data_df = LogReader.read_log_file(LOG_FILE, return_as_dataframe=True)
    logging.info(weather_data_df.head())
    logging.info(f"📈 Total rows to process: {len(weather_data_df)}")

    if len(weather_data_df) == 0:
        logging.warning("No data to process. Exiting.")
        return

    searcher = WeatherDataLocationSearcher(WOF_DELTA_PATH)
    delta_writer = DeltaWriter()
    clickhouse_writer = ClickHouseWriter(
        "127.0.0.1", "datasnake", "sensor_data_processed"
    )

    total_rows = len(weather_data_df)

    for start in range(0, total_rows, BATCH_SIZE):
        end = min(start + BATCH_SIZE, total_rows)
        batch_df = weather_data_df.slice(start, end - start)
        process_batch(batch_df, searcher, delta_writer, clickhouse_writer, start, end)

    logging.info("✅ All batches processed successfully.")


if __name__ == "__main__":
    main()
