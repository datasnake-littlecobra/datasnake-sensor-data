import pika
import json
import os
import logging
import re

from .raw_ground_postgres_writer import RawPostgresWriter

logging.basicConfig(level=logging.INFO)

RABBITMQ_HOST = "localhost"
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD")
VHOST = os.getenv("RABBITMQ_VHOST")

EXCHANGE = "sensor.ground.events"
ROUTING_KEY = "sensor.ground.raw"
QUEUE = "sensor_ground_raw"

POSTGRES_DSN = os.getenv("POSTGRES_DSN")

MESSAGE_RE = re.compile(r"message:\s*(\{.*\})\s*\|")


def connect_rabbitmq():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=VHOST,
        credentials=credentials,
    )
    return pika.BlockingConnection(params)


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


def on_message(channel, method, properties, body):
    try:
        outer = json.loads(body)
        raw_line = outer.get("raw", "")

        event = parse_raw_log_line(raw_line)

        raw_writer.write_event(event)

        logging.info(
            f"📥 RAW stored | device={event.get('device_id')} "
            f"lat={event.get('lat')} lon={event.get('lon')}"
        )

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception:
        logging.exception("❌ RAW consumer failed — message requeued")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    global raw_writer
    raw_writer = RawPostgresWriter(POSTGRES_DSN)

    connection = connect_rabbitmq()
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    channel.queue_declare(queue=QUEUE, durable=True)

    channel.queue_bind(exchange=EXCHANGE, queue=QUEUE, routing_key=ROUTING_KEY)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=QUEUE, on_message_callback=on_message)

    logging.info("🟢 RAW consumer started (Postgres writer inline)")
    channel.start_consuming()


if __name__ == "__main__":
    main()
