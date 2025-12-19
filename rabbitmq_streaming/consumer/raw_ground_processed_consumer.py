import pika
import json
import os
import logging
import re
from datetime import datetime
from pathlib import Path

import polars as pl
from geoprocessor.search_locations import WeatherDataLocationSearcher

# from utils.search_locations import WeatherDataLocationSearcher

logging.basicConfig(level=logging.INFO)

# -----------------------------
# RabbitMQ config
# -----------------------------
RABBITMQ_HOST = "localhost"
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD")
VHOST = os.getenv("RABBITMQ_VHOST")

EXCHANGE = "sensor.ground.events"
ROUTING_KEY = "sensor.ground.raw"
QUEUE = "sensor_ground_raw_processed"

# -----------------------------
# Processing config
# -----------------------------
WOF_DELTA_PATH = "/home/resources/deltalake-wof-oregon"

OUTPUT_LOG = Path("/home/dev/mqtt-python/processed_sensor_events.log")
OUTPUT_LOG.parent.mkdir(parents=True, exist_ok=True)

MESSAGE_RE = re.compile(r"message:\s*(\{.*\})\s*\|")


# -----------------------------
# RabbitMQ connection
# -----------------------------
def connect_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=VHOST,
        credentials=credentials,
    )
    return pika.BlockingConnection(params)


# -----------------------------
# Parsing raw payload
# -----------------------------
def parse_raw_log_line(raw_line: str) -> dict:
    event = {}

    # topic
    if "topic:" in raw_line:
        try:
            event["topic"] = raw_line.split("topic:")[1].split("|")[0].strip()
        except Exception:
            event["topic"] = "weather/data"
    else:
        event["topic"] = "weather/data"

    # embedded JSON
    match = MESSAGE_RE.search(raw_line)
    if match:
        event.update(json.loads(match.group(1)))

    return event


# -----------------------------
# Consumer callback
# -----------------------------
def on_message(channel, method, properties, body):
    try:
        outer = json.loads(body)
        raw_line = outer.get("raw", "")

        event = parse_raw_log_line(raw_line)

        # Convert single event → Polars DF
        df = pl.DataFrame([event])

        enriched_df = searcher.enrich_weather_data_optimized(df)

        if enriched_df.is_empty():
            logging.warning("⚠️ No enrichment result, skipping")
            channel.basic_ack(delivery_tag=method.delivery_tag)
            return

        # Write to log for now (Phase 1 validation)
        for row in enriched_df.iter_rows(named=True):
            record = {
                "processed_at": datetime.utcnow().isoformat(),
                **row,
            }
            OUTPUT_LOG.write_text(json.dumps(record) + "\n", append=True)

            logging.info(
                f"🧭 PROCESSED | device={row.get('device_id')} "
                f"postal={row.get('postal_code')} "
                f"lat={row.get('lat')} lon={row.get('lon')}"
            )

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logging.exception("❌ Processing consumer failed — message requeued")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


# -----------------------------
# Main
# -----------------------------
def main():
    global searcher
    searcher = WeatherDataLocationSearcher(WOF_DELTA_PATH)

    connection = connect_rabbitmq()
    channel = connection.channel()

    # Same exchange & routing key
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # Different queue = fan-out
    channel.queue_declare(queue=QUEUE, durable=True)

    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

    logging.info("🟢 Processed consumer started (log output only)")
    channel.start_consuming()


if __name__ == "__main__":
    main()
