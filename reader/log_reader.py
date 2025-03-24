import re
import json
import polars as pl
from pathlib import Path

try:
    from models.weather_data import WeatherData
    USE_WEATHER_MODEL = True
except ImportError:
    USE_WEATHER_MODEL = False

class LogReader:
    LOG_PATTERN = re.compile(
        r"(?:\[(.*?)\])?\s*topic:\s*weather/data\s*\|\s*message:\s*(\{.*?\})"
        r"(?:\s*\|\s*timestamp:\s*(\S+))?", 
        re.IGNORECASE
    )

    @staticmethod
    def read_log_file(filepath, return_as_dataframe=False):
        """Reads a log file and returns extracted weather data as a list or Polars DataFrame."""
        weather_data_list = []

        with Path(filepath).open("r") as file:
            for line in file:
                match = LogReader.LOG_PATTERN.search(line)
                if match:
                    timestamp = match.group(1) or match.group(3)  # Prefer timestamp from square brackets, else take from key
                    json_str = match.group(2)
                    # print(json_str)

                    if USE_WEATHER_MODEL:
                        weather_data = WeatherData.from_json(json_str, timestamp)
                    else:
                        data = json.loads(json_str)
                        data["timestamp"] = timestamp
                        weather_data = data

                    weather_data_list.append(weather_data)

        if return_as_dataframe:
            df = pl.DataFrame([data.to_dict() if USE_WEATHER_MODEL else data for data in weather_data_list])
            return df

        return weather_data_list
