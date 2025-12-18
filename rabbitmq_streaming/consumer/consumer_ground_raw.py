import pika
import json
import os

RABBITMQ_HOST = "localhost"
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASSWORD")
VHOST = os.getenv("RABBITMQ_VHOST")

EXCHANGE = "sensor.ground.events"
ROUTING_KEY = "sensor.ground.raw"
QUEUE = "sensor_ground_raw"


def connect():
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        virtual_host=VHOST,
        credentials=credentials
    )
    return pika.BlockingConnection(params)


def on_message(channel, method, properties, body):
    try:
        message = json.loads(body)
        print("📥 Received:", message)

        # TODO: insert into Postgres here

        channel.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print("❌ Error processing message:", e)
        # Message will be re-queued
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def main():
    connection = connect()
    channel = connection.channel()

    # Declare exchange (safe)
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic",
        durable=True
    )

    # Declare queue (consumer-owned)
    channel.queue_declare(
        queue=QUEUE,
        durable=True
    )

    # Bind queue to exchange
    channel.queue_bind(
        exchange=EXCHANGE,
        queue=QUEUE,
        routing_key=ROUTING_KEY
    )

    # Fair dispatch
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=QUEUE,
        on_message_callback=on_message
    )

    print("🟢 Waiting for ground sensor raw messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
