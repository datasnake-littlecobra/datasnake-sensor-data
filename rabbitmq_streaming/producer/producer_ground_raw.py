from datetime import datetime
import pika
import time
import json
from pathlib import Path
import os


RABBITMQ_HOST = "localhost"
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD")
VHOST = os.getenv("RABBITMQ_VHOST")

EXCHANGE = "sensor.ground.events"
ROUTING_KEY = "sensor.ground.raw"

LOG_FILE_PATH = "/home/dev/mqtt-python/mqtt_weather_distributed_logs_small.log"
LOG_FILE = Path(LOG_FILE_PATH)
POLL_INTERVAL = 10  # seconds


def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, virtual_host=VHOST, credentials=credentials
    )
    return pika.BlockingConnection(params)


def main():
    connection = connect()
    channel = connection.channel()

    # Declare exchange (safe to call multiple times)
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    last_size = 0

    while True:
        if LOG_FILE.exists():
            size = LOG_FILE.stat().st_size

            if size > last_size:
                with LOG_FILE.open() as f:
                    f.seek(last_size)
                    for line in f:
                        payload = {"source": "ground_sensor", "raw": line.strip()}

                        channel.basic_publish(
                            exchange=EXCHANGE,
                            routing_key=ROUTING_KEY,
                            body=json.dumps(payload),
                            properties=pika.BasicProperties(
                                delivery_mode=2  # persistent
                            ),
                        )
                        # If you want to detect dropped messages, you can publish with:
                        # channel.basic_publish(
                        #     exchange=EXCHANGE,
                        #     routing_key=ROUTING_KEY,
                        #     body=json.dumps(payload),
                        #     mandatory=True
                        # )


                        print(
                            f"📤 Produced @ {datetime.utcnow().isoformat()} | "
                            f"exchange={EXCHANGE} routing_key={ROUTING_KEY}"
                        )

                last_size = size

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
