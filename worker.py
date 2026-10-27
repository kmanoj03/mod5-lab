"""Order Worker: consumes order messages from the RabbitMQ `orders` queue.

Usage:
    python worker.py --name worker-1
    python worker.py --name worker-2 --delay 2
    python worker.py --name worker-1 --crash-before-ack     # failure / redelivery demo
    python worker.py --name worker-1 --crash-after-work     # redelivered duplicate demo

Key behaviours
  * manual ACK, sent only after processing has finished
  * prefetch=1, so a busy worker never hoards messages (fair competing consumers)
  * duplicate detection on event_id, shared by every worker through one file
"""
import argparse
import fcntl
import json
import logging
import os
import time
from pathlib import Path

import config


class IdempotencyStore:
    """Remembers which event_ids have been fully processed.

    State is a plain text file (one event_id per line) so that *all* worker
    processes share it. A per-event file lock stops two workers from running
    the business action for the same event_id at the same time; the lock is
    released by the OS if a worker dies.
    """

    def __init__(self, path: str):
        self.path = Path(path)
        self.lock_dir = Path(str(path) + ".locks")
        self.lock_dir.mkdir(exist_ok=True)
        self.path.touch(exist_ok=True)

    def is_processed(self, event_id: str) -> bool:
        with self.path.open() as f:
            return any(line.strip() == event_id for line in f)

    def mark_processed(self, event_id: str) -> None:
        with self.path.open("a") as f:
            f.write(event_id + "\n")
            f.flush()
            os.fsync(f.fileno())

    def lock(self, event_id: str):
        """Context manager: exclusive lock for one event_id (blocks if held)."""
        return _EventLock(self.lock_dir / f"{event_id}.lock")


class _EventLock:
    def __init__(self, path: Path):
        self.path = path
        self.fd = None

    def __enter__(self):
        self.fd = open(self.path, "w")
        fcntl.flock(self.fd, fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc):
        fcntl.flock(self.fd, fcntl.LOCK_UN)
        self.fd.close()


def business_action(order: dict, delay: float) -> None:
    """The side effect we must not repeat. Simulated as slow work (e.g. reserving stock)."""
    time.sleep(delay)


def make_callback(store: IdempotencyStore, log, delay: float,
                  crash_before_ack: bool = False, crash_after_work: bool = False):
    def on_message(channel, method, properties, body):
        redelivered = method.redelivered
        try:
            order = json.loads(body)
            event_id = order["event_id"]
        except (ValueError, KeyError, TypeError):
            log.error("malformed message, rejecting without requeue: %r", body)
            channel.basic_nack(method.delivery_tag, requeue=False)
            return

        log.info(
            "RECEIVED  event_id=%s order_id=%s item_id=%s qty=%s%s",
            event_id, order.get("order_id"), order.get("item_id"), order.get("quantity"),
            "  [REDELIVERED]" if redelivered else "",
        )

        with store.lock(event_id):
            if store.is_processed(event_id):
                log.warning(
                    "DUPLICATE event_id=%s already processed -> skipping business action",
                    event_id,
                )
                channel.basic_ack(method.delivery_tag)
                log.info("ACK       event_id=%s (duplicate discarded)", event_id)
                return

            log.info("PROCESSING order_id=%s item_id=%s qty=%s ...",
                     order.get("order_id"), order.get("item_id"), order.get("quantity"))

            if crash_before_ack:
                # Simulate a worker that dies mid-processing: no mark, no ACK.
                time.sleep(delay / 2)
                log.error("SIMULATED CRASH: exiting before ACK for event_id=%s "
                          "(broker will redeliver it)", event_id)
                os._exit(1)  # hard exit: no cleanup, TCP connection just drops

            business_action(order, delay)
            store.mark_processed(event_id)   # record only after the work is done
            log.info("DONE      order_id=%s processed", order.get("order_id"))

            if crash_after_work:
                # Work is done and recorded, but the ACK never reaches the broker.
                log.error("SIMULATED CRASH: work done but exiting before ACK for event_id=%s "
                          "(redelivery will be a duplicate)", event_id)
                os._exit(1)

        channel.basic_ack(method.delivery_tag)  # ACK strictly after processing finished
        log.info("ACK       event_id=%s", event_id)

    return on_message


def main() -> None:
    parser = argparse.ArgumentParser(description="RabbitMQ order worker")
    parser.add_argument("--name", default=f"worker-{os.getpid()}", help="label used in log lines")
    parser.add_argument("--delay", type=float, default=1.0, help="seconds of simulated work per order")
    parser.add_argument("--crash-before-ack", action="store_true",
                        help="die while handling the first message, before sending the ACK")
    parser.add_argument("--crash-after-work", action="store_true",
                        help="finish and record the first message, then die before sending the ACK")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s.%(msecs)03d [{args.name} pid={os.getpid()}] %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("pika").setLevel(logging.WARNING)
    log = logging.getLogger(args.name)

    store = IdempotencyStore(config.PROCESSED_FILE)
    connection = config.get_connection()
    channel = connection.channel()
    config.declare_queue(channel)
    channel.basic_qos(prefetch_count=1)  # one unacked message at a time per worker
    channel.basic_consume(
        queue=config.QUEUE_NAME,
        on_message_callback=make_callback(
            store, log, args.delay,
            crash_before_ack=args.crash_before_ack,
            crash_after_work=args.crash_after_work,
        ),
        auto_ack=False,                   # manual acknowledgements
    )

    log.info("waiting for messages on queue '%s' (Ctrl+C to stop)", config.QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("stopping")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
