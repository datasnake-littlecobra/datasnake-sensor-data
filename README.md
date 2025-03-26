# datasnake-sensor-data

## TO-DOS
- add clustering to cassandra table
- cross-check the number of rows processed, stored into cassandra
- double check clustering, z ordering or liquid clustering to deltalake

## ERRORS
- select * from sensor_data_processed order by timestamp desc limit 10;
InvalidRequest: Error from server: code=2200 [Invalid query] message="ORDER BY is only supported when the partition key is restricted by an EQ or an IN."
