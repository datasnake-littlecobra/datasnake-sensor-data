import json
import random
import time
from datetime import datetime
from pathlib import Path

LOG_FILE = Path("/home/dev/mqtt-python/mqtt_weather_distributed_logs.log")

# Oregon bounding box
LAT_RANGE = (42.00, 46.30)
LON_RANGE = (-124.60, -116.50)

DEVICE_IDS = [
    f"device-{i:03d}" for i in range(1, 21)  # 20 sensors across Oregon
]


def generate_event():
    return {
        "device_id": random.choice(DEVICE_IDS),
        "temp": round(random.uniform(5, 32), 2),          # OR climate range
        "humidity": round(random.uniform(30, 95), 2),
        "pressure": round(random.uniform(980, 1035), 2),
        "lat": round(random.uniform(*LAT_RANGE), 5),
        "lon": round(random.uniform(*LON_RANGE), 5),
        "alt": round(random.uniform(0, 3000), 2),         # sea → mountains
        "sats": random.randint(0, 12),
        "wind_speed": round(random.uniform(0, 35), 2),
        "wind_direction": random.randint(0, 360),
    }


def main():
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    while True:
        event = generate_event()
        timestamp = datetime.utcnow().isoformat()

        log_line = (
            f"[{timestamp}] topic: weather/data | "
            f"message: {json.dumps(event)} | "
            f"timestamp: {timestamp}\n"
        )

        with LOG_FILE.open("a") as f:
            f.write(log_line)

        # 👇 Useful human-readable debug log
        print(
            f"🛰️  Generated {event['device_id']} | "
            f"lat={event['lat']} lon={event['lon']} | "
            f"temp={event['temp']}C wind={event['wind_speed']}km/h"
        )

        time.sleep(1)


if __name__ == "__main__":
    main()
