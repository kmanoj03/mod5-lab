"""Order Producer: publishes order messages to the RabbitMQ `orders` queue.

Usage:
    python producer.py                    # publish 10 orders
    python producer.py --count 20         # publish 20 orders
    python producer.py --duplicate        # also re-publish the first order (same event_id)
"""
import argparse
import json
import logging
import random
import uuid

import pika

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [producer] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("producer")

ITEMS = ["item-100", "item-200", "item-300", "item-400", "item-500"]


def make_order(n: int) -> dict:
    return {
        "event_id": str(uuid.uuid4()),   # unique per business event -> idempotency key
        "order_id": f"order-{n:04d}",
        "item_id": random.choice(ITEMS),
        "quantity": random.randint(1, 5),
    }


def publish(channel, order: dict, tag: str = "") -> None:
    channel.basic_publish(
        exchange="",                       # default exchange -> routes by queue name
        routing_key=config.QUEUE_NAME,
        body=json.dumps(order).encode(),
        properties=pika.BasicProperties(
            content_type="application/json",
            delivery_mode=pika.DeliveryMode.Persistent,  # survive broker restart
        ),
    )
    log.info("published%s %s", f" ({tag})" if tag else "", json.dumps(order))


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish order messages to RabbitMQ")
    parser.add_argument("--count", type=int, default=10, help="number of orders to publish")
    parser.add_argument(
        "--duplicate",
        action="store_true",
        help="after publishing, re-publish the first order again (same event_id) "
             "to demonstrate duplicate handling",
    )
    args = parser.parse_args()

    connection = config.get_connection()
    channel = connection.channel()
    config.declare_queue(channel)
    channel.confirm_delivery()  # broker confirms each publish; raises if it is not routed/accepted

    first_order = None
    for n in range(1, args.count + 1):
        order = make_order(n)
        first_order = first_order or order
        publish(channel, order)

    if args.duplicate and first_order:
        publish(channel, first_order, tag="DUPLICATE of first order")

    total = args.count + (1 if args.duplicate and first_order else 0)
    log.info("done: %d message(s) published to queue '%s'", total, config.QUEUE_NAME)
    connection.close()


if __name__ == "__main__":
    main()
