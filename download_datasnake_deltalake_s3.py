import polars as pl
from deltalake import DeltaTable
import os

# Your S3 details
s3_bucket = "your-bucket-name"
s3_path = "s3://your-bucket-name/path-to-delta-table"

# access_key = os.getenv("AWS_ACCESS_KEY")
# secret_key = os.getenv("AWS_SECRET_KEY")
hostname = "sjc1.vultrobjects.com"
sensor_delta_s3_bucket_raw_prod = f"datasnake"
sensor_delta_s3_key_raw_prod = f"deltalake_sensor_data_processed"
sensor_s3_uri = f"s3://{sensor_delta_s3_bucket_raw_prod}/{sensor_delta_s3_key_raw_prod}"

sensor_data_processed_local_path = "deltalake-sensor-data-processed"

storage_options = {
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY"),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_KEY"),
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}
# Read Delta Table from S3
dt = DeltaTable(sensor_s3_uri, storage_options=storage_options)

# Convert to Polars DataFrame
df = pl.from_arrow(dt.to_pyarrow_table())

# Save locally as CSV or Parquet
df.write_csv("deltalake-sensor-data-processed.csv")
# df.write_parquet("weather_data.parquet")

print("Downloaded Delta Lake data successfully!")
