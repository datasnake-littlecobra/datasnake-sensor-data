import os
from deltalake import DeltaTable
import polars as pl
import geopandas as gpd
from shapely.geometry import Point
from shapely import wkb
from shapely.wkt import loads as wkt_loads
import duckdb
from datetime import datetime, timedelta
from DataFrameCache import DataFrameCache

con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL delta; LOAD delta;")

# ✅ Define Paths for GADM & WOF Data
# read_paths = {
#     "ADM0": "/home/resources/geoBoundariesCGAZ_ADM0.gpkg",
#     "ADM1": "/home/resources/geoBoundariesCGAZ_ADM1.gpkg",
#     "ADM2": "/home/resources/geoBoundariesCGAZ_ADM2.gpkg",
#     "WOF": "/home/resources/deltalake-wof-oregon",
# }

read_paths = {
    "ADM0": "/home/resources/geoBoundariesCGAZ_ADM0.gpkg",
    "ADM1": "/home/resources/geoBoundariesCGAZ_ADM1.gpkg",
    "ADM2": "/home/resources/geoBoundariesCGAZ_ADM2.gpkg",
    "WOF": "/home/resources/deltalake-wof-oregon",
}

read_paths_dev = {
    "ADM0": "C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\geoBoundariesCGAZ_ADM0.gpkg",
    "ADM1": "C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\geoBoundariesCGAZ_ADM1.gpkg",
    "ADM2": "C:\\datasnake\\prab\\dev\\datasnake-sensor-data\\geoBoundariesCGAZ_ADM2.gpkg",
    "WOF": "deltalake-wof-oregon",
}

dataframe_mapping = {
    "ADM0": [
        "country",
        "geom",
    ],
    "ADM1": [
        "state",
        "geom",
    ],
    "ADM2": [
        "county",
        "geom",
    ],
}
# ✅ Country Code Mapping
country_code_mapping = {
    "USA": "US",
    "GBR": "GB",
    "DEU": "DE",
    "FRA": "FR",
    "ESP": "ES",
    "ITA": "IT",
    "NLD": "NL",
    "CHN": "CN",
    "JPN": "JP",
    "CAN": "CA",
    "AUS": "AU",
    "BRA": "BR",
    "IND": "IN",
    "RUS": "RU",
    "MEX": "MX",
    "ZAF": "ZA",
}
gadm_cache = DataFrameCache(expiration_minutes=60)
wof_cache = DataFrameCache(expiration_minutes=60)
wof_location_cache = DataFrameCache(expiration_minutes=60)
wof_cache_key_full = f"wof_cache_full"
FAILED_RECORDS_FILE = "failed_records.csv"

import geopolars as gpl


class WeatherDataLocationSearcher:
    def __init__(self, delta_wof_path):
        self.wof_df = pl.scan_delta(delta_wof_path).collect()
        # cached_data = wof_cache.get(wof_cache_key)
        # if cached_data is not None:
        #     print(f"Using cached data for: {delta_wof_path}")
        #     self.wof_df = cached_data
        # else:
        #     print("read fresh deltatable and set cache:")
        #     self.wof_df = pl.scan_delta(delta_wof_path).collect()
        #     wof_cache.set(wof_cache_key, self.wof_df)

    def query_gadm_level(
        self, read_path, lat, long, level, geom_column, extract_column
    ):
        # print("inside query_gadm_level:", read_path)

        gadm_cache_key = (long, lat, level)
        # print("cache key:", gadm_cache_key)

        # Check the cache using the correct cache object
        cached_df = gadm_cache.get(gadm_cache_key)
        if cached_df is not None:
            # print("reading from gadm_cache", cached_df)
            return pl.from_pandas(cached_df)  # Convert back to Polars DataFrame

        if not all([read_path, lat, long, geom_column]):
            print(
                f"Invalid parameters for querying {level}: {read_path}, {lat}, {long}, {geom_column}"
            )
            return None

        # Construct the query
        gadm_query = f"""
        SELECT {extract_column} FROM ST_Read('{read_path}') 
        WHERE ST_Contains({geom_column}, ST_Point({long}, {lat}))
        """

        # Execute the query and get the DataFrame
        gadm_df = con.execute(gadm_query).pl()
        # print("gadm_df.head() after duckdb search:")
        # print(gadm_df.head())

        # Set the cache with the DataFrame converted to Pandas
        # print("setting gadm cache")
        gadm_cache.set(
            gadm_cache_key, gadm_df.to_pandas()
        )  # Convert to Pandas DataFrame for caching

        if gadm_df.is_empty():
            print(f"No match found in {level} for ({lat}, {long})")
            return None

        return gadm_df

    def find_location(self, lat, lon):
        """Returns (country, state, city) based on lat/lon using GADM layers."""

        adm0_result = self.query_gadm_level(
            read_paths["ADM0"], lat, lon, "ADM0", "geom", "shapeGroup"
        )

        # print(adm0_result.head())
        if adm0_result is None or adm0_result.is_empty():
            return None, None, None

        country = adm0_result["shapeGroup"][0]
        country = country_code_mapping[country]
        # print("found ADM0 country:", country)

        adm1_result = self.query_gadm_level(
            read_paths["ADM1"], lat, lon, "ADM1", "geom", "shapeName"
        )

        # print(adm0_result.head())
        if adm1_result is None or adm1_result.is_empty():
            return None, None, None

        state = adm1_result["shapeName"][0]
        # print("found ADM1 state:", state)

        adm2_result = self.query_gadm_level(
            read_paths["ADM2"], lat, lon, "ADM2", "geom", "shapeName"
        )

        # print(adm0_result.head())
        if adm2_result is None or adm2_result.is_empty():
            return None, None, None

        city_county = adm2_result["shapeName"][0]
        # print("found ADM1 state:", state)

        return country, state, city_county

    def query_gadm_level_geopolars(
        self, read_path, lat, long, level, geom_column, extract_column
    ):
        # print("inside query_gadm_level geopandas:", read_path)

        gadm_cache_key = (long, lat, level)
        # print("cache key: ", gadm_cache_key)

        if gadm_cache.get(gadm_cache_key) is not None:
            # print("reading from gadm_cache", gadm_cache.get(gadm_cache_key))
            return gadm_cache.get(gadm_cache_key)

        else:
            if not all([read_path, lat, long, geom_column]):
                print(
                    f"Invalid parameters for querying {level}: {read_path}, {lat}, {long}, {geom_column}"
                )
                return None

            # Load GADM1 geopackage into GeoPolars
            df_gadm = gpl.read_file(read_paths[level])
            # Check if the geometry is in WKB format and convert to WKT
            if df_gadm.geometry.dtype == "object":  # Check if geometry is in WKB
                df_gadm["geometry"] = df_gadm["geometry"].apply(
                    wkb.loads
                )  # Convert WKB to Shapely geometries

            # Convert geometries to WKT
            df_gadm["geometry_wkt"] = df_gadm["geometry"].apply(lambda geom: geom.wkt)

            # Now you can work with the GeoDataFrame with WKT geometries
            # print(df_gadm[["geometry_wkt"]].head())
            search_point = Point(long, lat)
            # search_gdf = gpd.GeoDataFrame(geometry=[search_point], crs=df_gadm.crs)

            exact_matches = df_gadm[df_gadm.geometry.equals(search_point)]
            # print(exact_matches)

            return exact_matches

    def find_location_geopolars(self, lat, lon):
        """Returns (country, state, city) based on lat/lon using GADM layers."""
        # Convert to GeoPolars point
        point = Point(lon, lat)
        adm0_result = self.query_gadm_level_geopolars(
            read_paths["ADM0"], lat, lon, "ADM0", "geom", "shapeGroup"
        )

        # print(adm0_result)
        if adm0_result is None or adm0_result.is_empty():
            return None, None, None

        # Find the state polygon containing the point
        country = adm0_result.filter(adm0_result["geom"].contains(point))
        # print("country geopolars")
        # print(country)
        if not country.is_empty():
            country = country["shapeGroup"][0]
        country = country_code_mapping[country]
        # print("found ADM0 country:", country)

        adm1_result = self.query_gadm_level(
            read_paths["ADM1"], lat, lon, "ADM1", "geom", "shapeName"
        )

        # print(adm0_result.head())
        if adm1_result is None or adm1_result.is_empty():
            return None, None, None

        # Find the state polygon containing the point
        state = adm1_result.filter(adm1_result["geom"].contains(point))
        # print("state geopolars")
        # print(state)
        if not state.is_empty():
            state_name = state["shapeName"][0]
        # print("found ADM1 state:", state)

        return country, state, None

    def query_wof_level_deltatable_pyarrow_query(self, country, state, city, lat, lon):
        # print(f"inside query wof deltatable pyarrow :", state)
        # state_partition_path = f"/home/resources/deltalake-wof/country={country}/state={state}"
        wof_cache_country_state_key = f"{country}-{state}"
        wof_location_cache_country_state_lat_long_key = f"{country}-{state}-{lat}-{lon}"
        # print("wof_cache_country_state_key")
        # print(wof_cache_country_state_key)
        if not all([country, state, lat, lon]):
            return None

        try:
            cached_df = wof_cache.get(wof_cache_country_state_key)
            cached_location = wof_cache.get(
                wof_location_cache_country_state_lat_long_key
            )
            if cached_location is not None:
                # print("returning the cached location:", cached_location)
                return
            if cached_df is not None:
                # print("Reading from wof_cache:")
                search_point = Point(lon, lat)

                for row in cached_df.iter_rows(named=True):
                    geom = row.get("geometry")
                    if geom and (
                        geom.contains(search_point) or geom.equals(search_point)
                    ):
                        return pl.DataFrame(row)

            self.wof_df = self.wof_df.filter(
                (pl.col("country") == country) & (pl.col("state") == state)
            )
            self.wof_df = self.wof_df.with_columns(
                pl.Series(
                    "geometry",
                    [wkt_loads(wkt) for wkt in self.wof_df["wkt_geometry"]],
                )
            )
            wof_cache.set(wof_cache_country_state_key, self.wof_df)
            # print("what wof cache did you set: ", wof_cache.get(wof_cache_country_state_key))
            search_point = Point(lon, lat)

            for row in self.wof_df.iter_rows(named=True):
                geom = row.get("geometry")
                if geom and (geom.contains(search_point) or geom.equals(search_point)):
                    wof_location_cache.set(
                        wof_location_cache_country_state_lat_long_key, pl.DataFrame(row)
                    )
                    return pl.DataFrame(row)
        except Exception as e:
            print(f"Error in WOF query: {e}")

        return None

    # def enrich_weather_data_batch(self, weather_data_df, batch_size=500):
    #     """Enrich weather data by finding locations in batches"""

    #     enriched_rows = []  # Store results to build a new DataFrame

    #     # Process in batches
    #     for batch in weather_data_df.iter_slices(batch_size):
    #         batch_results = []

    #         for row in batch.iter_rows(named=True):
    #             country, state, city = self.find_location(row["lat"], row["lon"])
    #             if country and state:
    #                 # print("all country,state and city, so use wkt_geometry_city")
    #                 # print(
    #                 #     f"query_wof_level_deltatable_pyarrow_query({country}, {state}, {city}, {row["lat"]}, {row["lon"]})"
    #                 # )
    #                 wof_result_polars_query = self.query_wof_level_deltatable_pyarrow_query(
    #                     country, state, city, row["lat"], row["lon"]
    #                 )
    #                 # print("wof_result_polars_query:")
    #                 # print(wof_result_polars_query)
    #                 enriched_rows.append({
    #                     "city": wof_result_polars_query["city"],
    #                     "state": wof_result_polars_query["state"],
    #                     "country": wof_result_polars_query["country"],
    #                     "postal_code": wof_result_polars_query["postal_code"],
    #                     "lat": wof_result_polars_query["lat"],
    #                     "lon": wof_result_polars_query["lon"]
    #                 })
    #                 # enriched_rows.append(pl.DataFrame(batch_results))
    #                 # return

    #             # batch_results.append({
    #             #     "city": city,
    #             #     "state": state,
    #             #     "country": country
    #             # })

    #         # Convert batch results to a Polars DataFrame and append to enriched_rows
    #         # enriched_rows.append(pl.DataFrame(batch_results))

    #     # Concatenate all enriched rows into a final DataFrame
    #     enriched_df = pl.DataFrame([data.to_dict() for data in enriched_rows])

    #     # Merge with the original DataFrame
    #     return enriched_df

    def enrich_weather_data(self, weather_data_df):
        """Enrich weather data and return as a Polars DataFrame."""
        try:
            if weather_data_df is None or weather_data_df.is_empty():
                return pl.DataFrame([])

            enriched_rows = []

            for row in weather_data_df.iter_rows(named=True):
                country, state, city = self.find_location(row["lat"], row["lon"])
                # print(f"{country} | {state} || {city} |||")
                if not country or not state:
                    continue
                if country and state:
                    # print("all country,state and city, so use wkt_geometry_city")
                    # print(
                    #     f"query_wof_level_deltatable_pyarrow_query({country}, {state}, {city}, {row["lat"]}, {row["lon"]})"
                    # )
                    wof_result_polars_query = (
                        self.query_wof_level_deltatable_pyarrow_query(
                            country, state, city, row["lat"], row["lon"]
                        )
                    )
                    postal_code = None
                    if (
                        wof_result_polars_query is None
                        or wof_result_polars_query.is_empty()
                    ):
                        print(
                            f"Warning: No postal_code found for {wof_result_polars_query['postal_code']} skipping..."
                        )
                        postal_code = None
                    else:
                        print(
                            f"Success: Found postal_code found for {wof_result_polars_query['postal_code']}"
                        )
                        postal_code = wof_result_polars_query["postal_code"][0]
                        enriched_rows.append(
                            {
                                "postal_code": postal_code,
                                "lat": row["lat"],
                                "lon": row["lon"],
                                "country": country,
                                "state": state,
                                "city": city,
                            }
                        )

                    # print("enriched_rows =>")
                    # print(type(enriched_rows))
                    # print(enriched_rows)
                else:
                    print("moving on to the next record")

            # Convert the list of enriched rows into a Polars DataFrame
            return pl.DataFrame(enriched_rows)
        except Exception as e:
            print(f"Error processing record : {e}")

            # # Save failed record to CSV
            # if not os.path.exists(FAILED_RECORDS_FILE):
            #     with open(FAILED_RECORDS_FILE, "w", newline="") as f:
            #         writer = csv.writer(f)
            #         writer.writerow(["lat", "lon", "error_message"])  # Write header

            # with open(FAILED_RECORDS_FILE, "a", newline="") as f:
            #     writer = csv.writer(f)
            #     writer.writerow([weather_data_entry["lat"], weather_data_entry["lon"], str(e)])

            # return None  # Skip this record

    def enrich_weather_data_optimized(self, weather_data_df):
        try:
            if weather_data_df is None or weather_data_df.is_empty():
                return pl.DataFrame([])

            enriched_rows = []
            count = 0
            for row in weather_data_df.iter_rows(named=True):
                count = count + 1
                print(row)
                lat, lon = row.get("lat"), row.get("lon")
                if lat is None or lon is None:
                    continue

                country, state, city = self.find_location(lat, lon)
                if not country or not state:
                    continue

                t1 = datetime.now()
                wof_result = self.query_wof_level_deltatable_pyarrow_query(
                    country, state, city, lat, lon
                )
                t2 = datetime.now()
                total = t2 - t1

                postal_code = (
                    wof_result["postal_code"].item(0)
                    if wof_result is not None and not wof_result.is_empty()
                    else None
                )
                print(
                    f"it took {total} to search wof_result for count {count} with postal code: {postal_code}"
                )
                enriched_rows.append(
                    {
                        "postal_code": postal_code,
                        "lat": lat,
                        "lon": lon,
                        "country": country,
                        "state": state,
                        "city": city,
                        "device_id": row.get("device_id"),
                        "temp": row.get("temp"),
                        "humidity": row.get("humidity"),
                        "pressure": row.get("pressure"),
                        "lat": row.get("lat"),
                        "lon": row.get("lon"),
                        "alt": row.get("alt"),
                        "sats": row.get("sats"),
                        "wind_speed": row.get("wind_speed"),
                        "wind_direction": row.get("wind_direction"),
                        "timestamp": row.get("timestamp"),
                    }
                )

            return pl.DataFrame(enriched_rows) if enriched_rows else pl.DataFrame([])

        except Exception as e:
            raise e
