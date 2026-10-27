"""Shared settings for the producer and the workers.

Everything can be overridden with environment variables so the same code
works against Docker, a Homebrew install, or a remote broker.
"""
import os

import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")

QUEUE_NAME = os.getenv("QUEUE_NAME", "orders")

# File shared by all workers to remember which event_ids were already handled.
PROCESSED_FILE = os.getenv("PROCESSED_FILE", "processed_events.txt")


def get_connection() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=30,
    )
    return pika.BlockingConnection(params)


def declare_queue(channel) -> None:
    """Durable queue; both producer and worker call this so start order is irrelevant."""
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
