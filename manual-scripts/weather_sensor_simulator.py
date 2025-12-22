import json
import random
import time
from datetime import datetime, timezone
from pathlib import Path

LOG_FILE = Path("/home/dev/mqtt-python/mqtt_weather_distributed_logs.log")

# -------------------------------------------------
# Fixed devices across Oregon (postal-friendly)
# -------------------------------------------------
DEVICES = {
    "device-001": {"lat": 45.5231, "lon": -122.6765},  # Portland
    "device-002": {"lat": 44.9429, "lon": -123.0351},  # Salem
    "device-003": {"lat": 44.0521, "lon": -123.0868},  # Eugene
    "device-004": {"lat": 44.0582, "lon": -121.3153},  # Bend
    "device-005": {"lat": 42.3265, "lon": -122.8756},  # Medford
}

# Initial weather state per device
STATE = {
    device_id: {
        "temp": random.uniform(8, 22),
        "humidity": random.uniform(45, 85),
        "pressure": random.uniform(990, 1025),
    }
    for device_id in DEVICES
}


def generate_event(device_id: str):
    """Generate slightly noisy weather readings for a fixed sensor."""
    state = STATE[device_id]

    # Smooth variation (no teleporting values)
    state["temp"] += random.uniform(-0.4, 0.4)
    state["humidity"] += random.uniform(-1.5, 1.5)
    state["pressure"] += random.uniform(-0.6, 0.6)

    return {
        "device_id": device_id,
        "temp": round(state["temp"], 2),
        "humidity": round(state["humidity"], 2),
        "pressure": round(state["pressure"], 2),
        "lat": DEVICES[device_id]["lat"],
        "lon": DEVICES[device_id]["lon"],
        "alt": random.uniform(10, 300),  # small variation
        "sats": random.randint(7, 12),
        "wind_speed": round(random.uniform(0, 25), 2),
        "wind_direction": random.randint(0, 360),
    }


def main():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    print("🛰️ Weather sensor simulator started")

    while True:
        for device_id in DEVICES:
            event = generate_event(device_id)
            timestamp = datetime.now(timezone.utc).isoformat()

            log_line = (
                f"[{timestamp}] topic: weather/data | "
                f"message: {json.dumps(event)} | "
                f"timestamp: {timestamp}\n"
            )

            with LOG_FILE.open("a") as f:
                f.write(log_line)

            print(
                f"📡 {device_id} | "
                f"lat={event['lat']} lon={event['lon']} | "
                f"temp={event['temp']}C humidity={event['humidity']}%"
            )

            # jitter so devices don't sync
            time.sleep(random.uniform(1.5, 3.5))

        # base cadence ≈ every 10 seconds per device
        time.sleep(10)


if __name__ == "__main__":
    main()
