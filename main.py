# main.py
import polars as pl
from datetime import datetime, timedelta
from reader.log_reader import LogReader
from storage.delta_writer import DeltaWriter
import logging
from storage.cassandra_writer import CassandraWriter
from geoprocessor.search_locations import WeatherDataLocationSearcher

LOG_FILE = "/home/dev/mqtt-python/mqtt_weather_logs.log"
PROCESSED_SENSOR_DATA_DELTA_PATH = "/datasnake-deltalake-sensor-data-processed"
CASSANDRA_KEYSPACE = "datasnake"
CASSANDRA_TABLE = "sensor_data_processed"
WOF_DELTA_PATH = "/home/resources/deltalake-wof-oregon"
# WOF_DELTA_PATH = "deltalake-wof-oregon"
# GADM_DELTA_PATH = "/home/resources/deltalake-wof-oregon"

# search_locations_columns_coordinates = ["lat", "lon"]


def main():
    # Read log file
    return_as_dataframe = True
    weather_data_df = LogReader.read_log_file(LOG_FILE, return_as_dataframe)
    # weather_data_df = pl.DataFrame(weather_data_df)
    logging.info(weather_data_df.head())
    logging.info(f"total weather_data_df length of rows : {len(weather_data_df)}")
    return
    # save to pre-processing deltalake / cassandra

    # Enrich with location
    searcher = WeatherDataLocationSearcher(WOF_DELTA_PATH)
    t1 = datetime.now()
    # enriched_weather_df = searcher.enrich_weather_data_batch(weather_data_df[search_locations_columns_coordinates], batch_size=500)
    t2 = datetime.now()
    total = t2 - t1
    logging.info(f"it took {total} to search the enrich_weather_data_batch")
    t1 = datetime.now()
    enriched_weather_df = searcher.enrich_weather_data_optimized(weather_data_df)
    t2 = datetime.now()
    total = t2 - t1
    logging.info(f"it took {total} to search the enrich_weather_data")
    logging.info(enriched_weather_df.head())
    logging.info(f"total length rows: {len(enriched_weather_df)}")
    
    return

    # handle nulls for deltalake valid data type storage
    enriched_weather_df = enriched_weather_df.with_columns(
        pl.col("city").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("state").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("country").cast(pl.Utf8).fill_null("Unknown"),
        pl.col("postal_code").cast(pl.Utf8).fill_null("00000"),
    )

    # Write to Delta Lake
    t1 = datetime.now()
    delta_writer = DeltaWriter()
    # delta_writer.write_to_deltalake_dev(enriched_weather_df)
    delta_writer.write_to_deltalake(enriched_weather_df)
    t2 = datetime.now()
    total = t2 - t1
    logging.info(f"it took {total} to insert into deltalake")

    # Write to Cassandra
    t1 = datetime.now()
    cassandra_writer = CassandraWriter(CASSANDRA_KEYSPACE, CASSANDRA_TABLE)
    cassandra_writer.write_to_cassandra_batch_concurrent(enriched_weather_df)
    t2 = datetime.now()
    total = t2 - t1
    logging.info(f"it took {total} to insert into cassandra")
    logging.info(enriched_weather_df.head())


if __name__ == "__main__":
    main()
