# storage/delta_writer.py
import polars as pl
import logging
import os
from deltalake import DeltaTable, write_deltalake

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)

access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_SECRET_KEY")
hostname = "sjc1.vultrobjects.com"

# print(f"Access Key: {access_key}")
# print(f"Secret Key: {secret_key}")

deltalake_gadm_s3_uri = {
    "ADM0": f"s3://deltalake-gadm/gadm0",
    "ADM1": f"s3://deltalake-gadm/gadm1",
    "ADM2": f"s3://deltalake-gadm/gadm2",
    "COMBINED": f"s3://deltalake-gadm/gadm_combined",
}

deltalake_wof_s3_uri = f"s3://deltalake-gadm/gadm_combined"

# Connect to the Cassandra cluster
# USERNAME = "cassandra"
# PASSWORD = "cassandra"
# auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
# cluster = Cluster(
#     ["127.0.0.1"], auth_provider=auth_provider
# )  # Replace with container's IP if needed
# session = cluster.connect()


# # Use the keyspace
# session.set_keyspace("test_keyspace")

sensor_delta_s3_bucket_raw_prod = f"datasnake"
sensor_delta_s3_key_raw_prod = f"deltalake_sensor_data_processed"
sensor_s3_uri = f"s3://{sensor_delta_s3_bucket_raw_prod}/{sensor_delta_s3_key_raw_prod}"

sensor_data_processed_local_path = "deltalake-sensor-data-processed"

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


class DeltaWriter:
    def __init__(self):
        pass

    def write_to_deltalake(self, weather_data_processed_df):
        print("writing to deltalake:", type(weather_data_processed_df))
        try:
            write_deltalake(
                table_or_uri=sensor_s3_uri,
                storage_options=storage_options,
                data=weather_data_processed_df,  # Convert Polars DataFrame to Arrow Table
                mode="append",
                partition_by=["country", "state"],  # Specify partition keys
            )
            logging.info(f"written successfully to deltalake")
        except Exception as e:
            print(f"exception occurred writing to processed sensor deltalake:", e)
            logging.info(
                f"exception occurred writing to processed sensor deltalake:", e
            )

    def write_to_deltalake_dev(self, weather_data_processed_df):
        print("writing to deltalake:", type(weather_data_processed_df))
        print("writing to deltalake:", len(weather_data_processed_df))
        try:
            write_deltalake(
                table_or_uri=sensor_data_processed_local_path,
                data=weather_data_processed_df,
                mode="append",
                partition_by=["country", "state"]
            )
            logging.info(f"written successfully to deltalake")
        except Exception as e:
            print(f"exception occurred writing to processed sensor deltalake:", e)
            logging.info(
                f"exception occurred writing to processed sensor deltalake:", e
            )
