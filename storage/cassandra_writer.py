# storage/cassandra_writer.py
import uuid
import logging
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
from concurrent.futures import ThreadPoolExecutor, as_completed
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args

class CassandraWriter:
    def __init__(self, keyspace, table):
        # self.cluster = Cluster(["127.0.0.1"])
        # self.session = self.cluster.connect()
        self.keyspace = keyspace
        self.table = table
        # self.session.set_keyspace(keyspace)
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        CASSANDRA_HOSTS = ["127.0.0.1"]
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            auth_provider=auth_provider,
            protocol_version=4,  # Stay on version 4
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        self.session = cluster.connect()
        self.session.set_keyspace(keyspace)
        print("cassandra connection established!")
        
    def row_generator(dataframe, columns):
        for row in dataframe.iter_rows(named=True):
            yield tuple(row[col] if col in row else None for col in columns)

    def write_to_cassandra_simple(self, weather_data_processed_df):
        try:
            print('keyspace:', self.keyspace)
            print('table:', self.table)
            query = f"""
                    INSERT INTO {self.keyspace}.{self.table} (
                        id, timestamp, topic, temp, humidity, pressure, lat, lon, alt, sats, 
                        county, city, state, country, postal_code, nearby_postal_codes, processed_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """
            prepared = self.session.prepare(query)

            data = []
            for row in weather_data_processed_df.to_dicts():
                data.append((
                    uuid.uuid4(),  # Generating a random UUID for the primary key
                    datetime.now(),  # Using current timestamp as processed timestamp
                    "weather/Data",
                    row.get("temp", 0.0),  # Default to 0.0 if missing
                    row.get("humidity", 0.0),  # Default to 0.0
                    row.get("pressure", 0.0),  # Default to 0.0
                    row.get("lat", 0.0),
                    row.get("lon", 0.0),
                    row.get("alt", 0.0),  # Default altitude
                    row.get("sats", 0),  # Default satellites count
                    "dummy_county",  # Dummy county
                    row.get("city", "unknown"),
                    row.get("state", "unknown"),
                    row.get("country", "unknown"),
                    row.get("postal_code", "00000"),
                    ["00001", "00002"],  # Dummy list of nearby postal codes
                    datetime.utcnow()  # Processed timestamp
                ))

            # Execute batch insert
            for row in data:
                self.session.execute(prepared, row)
                print("Data inserted successfully:", row)
        except Exception as e:
                print(f"exception occurred writing to processed sensor deltalake:" , e)
                
    def write_to_cassandra_batch_concurrent(self, weather_data_processed_df):
        try:
            # print('keyspace:', self.keyspace)
            # print('table:', self.table)
            query = f"""
                    INSERT INTO {self.keyspace}.{self.table} (
                        id, timestamp, topic, temp, humidity, pressure, lat, lon, alt, sats, 
                        county, city, state, country, postal_code, nearby_postal_codes, processed_at
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                    """
            prepared = self.session.prepare(query)

            data = []
            for row in weather_data_processed_df.to_dicts():
                data.append((
                    uuid.uuid4(),  # Generating a random UUID for the primary key
                    datetime.now(),  # Using current timestamp as processed timestamp
                    "weather/Data",
                    row.get("temp", 0.0),  # Default to 0.0 if missing
                    row.get("humidity", 0.0),  # Default to 0.0
                    row.get("pressure", 0.0),  # Default to 0.0
                    row.get("lat", 0.0),
                    row.get("lon", 0.0),
                    row.get("alt", 0.0),  # Default altitude
                    row.get("sats", 0),  # Default satellites count
                    "dummy_county",  # Dummy county
                    row.get("city", "unknown"),
                    row.get("state", "unknown"),
                    row.get("country", "unknown"),
                    row.get("postal_code", "00000"),
                    ["00001", "00002"],  # Dummy list of nearby postal codes
                    datetime.utcnow()  # Processed timestamp
                ))

            # Execute batch insert
            results = execute_concurrent_with_args(
                self.session, prepared, data, concurrency=20
            )
            
            # Log any errors
            for success, result in results:
                if success:
                    logging.info("Cassandra: Inserted all rows asynchronously.")
                if not success:
                    logging.error(f"Write failed: {result}")
        except Exception as e:
                print(f"exception occurred writing to processed sensor deltalake:" , e)