import json
import re
from datetime import datetime


class WeatherData:
    def __init__(
        self,
        device_id=None,
        temp=None,
        humidity=None,
        pressure=None,
        lat=None,
        lon=None,
        alt=None,
        sats=None,
        wind_speed=None,
        wind_direction=None,
        timestamp=None,
    ):
        self.device_id = device_id
        self.temp = temp
        self.humidity = humidity
        self.pressure = pressure
        self.lat = lat
        self.lon = lon
        self.alt = alt
        self.sats = sats
        self.wind_speed = wind_speed
        self.wind_direction = wind_direction
        self.timestamp = timestamp or datetime.utcnow().isoformat()

    @classmethod
    def from_json(cls, json_str, timestamp=None):
        """Creates WeatherData object from a JSON string, handling NaN and missing keys."""

        # Replace `NaN` (invalid JSON) with `null` (valid JSON)
        json_str = re.sub(r"\bNaN\b", "null", json_str, flags=re.IGNORECASE)

        # Parse JSON safely
        data = json.loads(json_str)

        return cls(
            device_id=data.get("device_id", None),
            temp=data.get("temp", None),
            humidity=data.get("humidity", None),
            pressure=data.get("pressure", None),
            lat=data.get("lat", None),
            lon=data.get("lon", None),
            alt=data.get("alt", None),
            sats=data.get("sats", None),
            wind_speed=data.get("wind_speed", None),
            wind_direction=data.get("wind_direction", None),
            timestamp=timestamp,
        )

    def to_dict(self):
        """Converts the object into a dictionary."""
        return {
            "device_id": self.device_id,
            "temp": self.temp,
            "humidity": self.humidity,
            "pressure": self.pressure,
            "lat": self.lat,
            "lon": self.lon,
            "alt": self.alt,
            "sats": self.sats,
            "wind_speed": self.wind_speed,
            "wind_direction": self.wind_direction,
            "timestamp": self.timestamp,
        }
