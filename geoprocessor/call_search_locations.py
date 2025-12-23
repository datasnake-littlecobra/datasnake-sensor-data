import logging
import polars as pl
from shapely.geometry import Point
from shapely.wkt import loads as wkt_loads
import duckdb

# DuckDB setup
con = duckdb.connect()
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL delta; LOAD delta;")

logging.basicConfig(level=logging.INFO)

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

class WeatherDataLocationSearcher:
    def __init__(self, wof_delta_path: str, gadm_paths: dict, pg_conn):
        """
        wof_delta_path: path to WOF delta table (Oregon only for now)
        gadm_paths: dict with keys ADM0, ADM1, ADM2 -> gpkg paths
        pg_conn: Postgres connection object
        """
        self.wof_delta_path = wof_delta_path
        self.gadm_paths = gadm_paths
        self.pg_conn = pg_conn.get_conn()

        logging.info("📦 Loading WOF Delta table (once, immutable)")
        self.wof_df = pl.scan_delta(self.wof_delta_path).collect()
        logging.info(f"📊 WOF rows loaded: {len(self.wof_df)}")

    def lookup_city_from_postgres(self, postal_code: str):
        if not postal_code:
            return None, None

        with self.pg_conn.cursor() as cur:
            cur.execute(
                """
                SELECT city, locale_name
                FROM usps_postal_code_mapping
                WHERE postal_code = %s
                LIMIT 1
                """,
                (postal_code,),
            )
            row = cur.fetchone()
            logging.info(f"🌍 City and locale_name lookup for postal code {postal_code}: {row}")

        return row if row else (None, None)


    # -----------------------------
    # GADM LOOKUPS (Country/State/County)
    # -----------------------------
    def _query_gadm(self, level: str, lat: float, lon: float, extract_col: str):
        path = self.gadm_paths.get(level)
        if not path:
            logging.warning(f"GADM path missing for {level}")
            return None

        query = f"""
        SELECT {extract_col}
        FROM ST_Read('{path}')
        WHERE ST_Contains(geom, ST_Point({lon}, {lat}))
        """

        logging.info(f"🌍 GADM {level} lookup for lat={lat}, lon={lon}")
        df = con.execute(query).pl()

        if df.is_empty():
            logging.warning(f"❌ No GADM {level} match")
            return None

        value = df[extract_col][0]
        logging.info(f"✅ GADM {level} match: {value}")
        return value

    def find_admin_regions(self, lat: float, lon: float):
        country = self._query_gadm("ADM0", lat, lon, "shapeGroup")
        if country:
            country = country_code_mapping.get(country, country)

        state = self._query_gadm("ADM1", lat, lon, "shapeName")
        county = self._query_gadm("ADM2", lat, lon, "shapeName")

        return country, state, county


    # -----------------------------
    # POSTAL CODE LOOKUP (NO CACHE)
    # -----------------------------
    def lookup_postal_code(self, lat: float, lon: float, country: str, state: str):
        """
        Deterministic, no-cache postal code lookup.
        """

        logging.info(
            f"📮 Postal lookup start | country={country}, state={state}, lat={lat}, lon={lon}"
        )

        # Filter WOF by country/state WITHOUT mutating global df
        state_df = self.wof_df.filter(
            (pl.col("country") == country) & (pl.col("state") == state)
        )

        logging.info(f"📂 WOF rows after state filter: {len(state_df)}")

        if state_df.is_empty():
            logging.warning("❌ No WOF rows for state")
            return None

        # Convert geometries
        logging.info("🧱 Converting WKT geometries to Shapely polygons")
        geometries = [wkt_loads(wkt) for wkt in state_df["wkt_geometry"]]
        state_df = state_df.with_columns(pl.Series("geometry", geometries))

        point = Point(lon, lat)
        logging.info(f"📍 Search point: {point}")

        # Polygon match loop (slow but explicit)
        for idx, row in enumerate(state_df.iter_rows(named=True)):
            geom = row["geometry"]
            postal = row.get("postal_code")

            if geom is None:
                continue

            if geom.contains(point) or geom.touches(point):
                logging.info(f"🎯 POSTAL MATCH FOUND | index={idx}, postal={postal}")
                return postal

        logging.warning("⚠️ No postal polygon matched this coordinate")
        return None

    # -----------------------------
    # MAIN ENTRY POINT
    # -----------------------------
    def enrich_single_record(self, raw_ground_df: pl.DataFrame) -> pl.DataFrame:
        logging.info("🚀 Starting enrichment")

        row = raw_ground_df.to_dicts()[0]
        lat = row.get("lat")
        lon = row.get("lon")
        if not lat or not lon:
            logging.warning("❌ Missing lat/lon — skipping enrichment")
            return None
        
        enriched_rows = []
        country, state, county = self.find_admin_regions(lat, lon)

        if not country or not state:
            logging.warning("❌ Missing country/state — skipping postal lookup")
            return None

        postal_code = self.lookup_postal_code(lat, lon, country, state)
        
        city, locale_name = self.lookup_city_from_postgres(postal_code)

        enriched_rows.append(
            {
                "postal_code": postal_code,
                "lat": lat,
                "lon": lon,
                "usps_locale_name": locale_name,
                "country": country,
                "state": state,
                "city": city,
                "county": county,
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
        logging.info(f"Length of enriched_rows array: {len(enriched_rows)}")
        return pl.DataFrame(enriched_rows) if enriched_rows else pl.DataFrame([])
