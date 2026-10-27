# Mod 5 Lab: Asynchronous Order Messaging with RabbitMQ (Python)

An **Order Producer** (`producer.py`) publishes order messages (`event_id`, `order_id`, `item_id`, `quantity`) to a RabbitMQ queue, and **Order Workers** (`worker.py`) consume them as independent processes. Workers use manual ACKs and skip duplicate `event_id`s.

## How to run

**1. Start RabbitMQ**

```bash
brew install rabbitmq
brew services start rabbitmq
```

The management UI is at http://localhost:15672 (login `guest` / `guest`).

**2. Install dependencies**

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**3. Start two workers** (each in its own terminal)

```bash
python worker.py --name worker-1
python worker.py --name worker-2
```

**4. Publish orders** (in a third terminal)

```bash
python producer.py --count 10
```

**Failure / redelivery:** start `python worker.py --name worker-1 --crash-before-ack` first, publish with `python producer.py --count 3`, then start `python worker.py --name worker-2`. worker-1 receives a message and exits before ACKing it, and worker-2 gets the message again.

**Duplicates:** run `python producer.py --count 4 --duplicate` to re-publish the first order with the same `event_id`.

**All demos automatically:** `./scripts/run_demos.sh` runs all three scenarios and writes the logs to `evidence/`.

## Architecture

```
                                 RabbitMQ
 +----------------+  publish   +-------------------+  deliver (prefetch=1)  +-----------+
 | Order Producer | ---------> |  queue: "orders"  | ---------------------> | worker-1  |---+
 |  producer.py   |            |  (durable)        | ---------------------> | worker-2  |---+
 +----------------+            +-------------------+ <---------------------  +-----------+   |
                                                       ACK after processing                  |
                                                                                             v
                                                          processed_events.txt (shared event_id store)
```

- **Competing consumers:** each message goes to only one worker.
- **ACK timing:** a worker ACKs only after processing finishes.
- **Redelivery:** if a worker dies before ACKing, RabbitMQ puts the message back in the queue and delivers it to another worker.
- **Duplicate check:** before doing the work, a worker looks up the message's `event_id` in `processed_events.txt`. If it's already there, the worker logs a duplicate and ACKs without processing.

## Evidence

The full logs are in [`evidence/`](evidence). The excerpts below are copied from those logs.

### Successful message processing

From [`evidence/01_competing_consumers.log`](evidence/01_competing_consumers.log):

```
22:05:01.873 [producer] published {"event_id": "5cbbe8b6-bc62-440f-aae6-e0e696e1a47d", "order_id": "order-0001", "item_id": "item-100", "quantity": 5}
22:05:01.873 [worker-1 pid=1795] RECEIVED  event_id=5cbbe8b6-bc62-440f-aae6-e0e696e1a47d order_id=order-0001 item_id=item-100 qty=5
22:05:01.873 [worker-1 pid=1795] PROCESSING order_id=order-0001 item_id=item-100 qty=5 ...
22:05:02.875 [worker-1 pid=1795] DONE      order_id=order-0001 processed
22:05:02.875 [worker-1 pid=1795] ACK       event_id=5cbbe8b6-bc62-440f-aae6-e0e696e1a47d
```

### Two competing consumers

Both workers consumed the same `orders` queue, and the producer published 10 orders. Each order was processed by exactly one worker. From [`evidence/01_competing_consumers.log`](evidence/01_competing_consumers.log):

```
===== queue before publishing =====
name	messages_ready	messages_unacknowledged	consumers
orders	0	0	2

22:05:01.873 [worker-1 pid=1795] RECEIVED  event_id=5cbbe8b6-... order_id=order-0001 item_id=item-100 qty=5
22:05:01.874 [worker-2 pid=1796] RECEIVED  event_id=2bd7a5d8-... order_id=order-0002 item_id=item-500 qty=2
22:05:02.877 [worker-1 pid=1795] RECEIVED  event_id=b42608d3-... order_id=order-0003 item_id=item-500 qty=1
22:05:02.881 [worker-2 pid=1796] RECEIVED  event_id=8fba1774-... order_id=order-0004 item_id=item-400 qty=2
...

===== summary =====
worker-1 processed 5 orders: order-0001 order-0003 order-0005 order-0007 order-0009
worker-2 processed 5 orders: order-0002 order-0004 order-0006 order-0008 order-0010
orders published:          10
orders processed (total):  10
distinct order_ids done:   10
```

### Failure and redelivery

worker-1 was started with `--crash-before-ack`. It received `order-0001` and exited before sending the ACK. RabbitMQ then redelivered the message to worker-2. From [`evidence/02_failure_redelivery.log`](evidence/02_failure_redelivery.log):

```
===== queue while worker-1 holds order-0001 unacknowledged =====
name	messages_ready	messages_unacknowledged	consumers
orders	2	1	1

22:05:11.094 [worker-1 pid=2121] RECEIVED  event_id=cbe56a77-d205-4558-9973-c3a0629b0d7d order_id=order-0001 item_id=item-200 qty=4
22:05:11.094 [worker-1 pid=2121] PROCESSING order_id=order-0001 item_id=item-200 qty=4 ...
22:05:13.108 [worker-1 pid=2121] SIMULATED CRASH: exiting before ACK for event_id=cbe56a77-d205-4558-9973-c3a0629b0d7d (broker will redeliver it)
22:05:13.138 [worker-2 pid=2170] RECEIVED  event_id=cbe56a77-d205-4558-9973-c3a0629b0d7d order_id=order-0001 item_id=item-200 qty=4  [REDELIVERED]
22:05:13.138 [worker-2 pid=2170] PROCESSING order_id=order-0001 item_id=item-200 qty=4 ...
22:05:14.140 [worker-2 pid=2170] DONE      order_id=order-0001 processed
22:05:14.140 [worker-2 pid=2170] ACK       event_id=cbe56a77-d205-4558-9973-c3a0629b0d7d
```

The system kept running after the crash. worker-1 was restarted, and both workers shared the next batch of orders:

```
22:05:18.744 [worker-1-restarted pid=2297] DONE      order_id=order-0005 processed
22:05:18.744 [worker-2 pid=2170] DONE      order_id=order-0004 processed
22:05:19.754 [worker-1-restarted pid=2297] DONE      order_id=order-0006 processed
22:05:19.757 [worker-2 pid=2170] DONE      order_id=order-0007 processed

===== queue at the end =====
name	messages_ready	messages_unacknowledged	consumers
orders	0	0	0
```

### Duplicate handling

Processed `event_id`s are stored in `processed_events.txt`, which all workers share. From [`evidence/03_duplicate_handling.log`](evidence/03_duplicate_handling.log):

**A. Same event published twice.** worker-2 processed `order-0001`. When the duplicate copy arrived, worker-1 detected it and skipped the business action:

```
22:05:25.123 [producer] published (DUPLICATE of first order) {"event_id": "f7e74fc5-3b08-427b-87cd-5c527d9ce7c4", "order_id": "order-0001", ...}
22:05:26.120 [worker-2 pid=2527] DONE      order_id=order-0001 processed
22:05:27.127 [worker-1 pid=2526] RECEIVED  event_id=f7e74fc5-3b08-427b-87cd-5c527d9ce7c4 order_id=order-0001 item_id=item-100 qty=3
22:05:27.127 [worker-1 pid=2526] DUPLICATE event_id=f7e74fc5-3b08-427b-87cd-5c527d9ce7c4 already processed -> skipping business action
22:05:27.127 [worker-1 pid=2526] ACK       event_id=f7e74fc5-3b08-427b-87cd-5c527d9ce7c4 (duplicate discarded)

messages published:            5
business actions executed:     4
duplicates detected/skipped:   1
```

**B. Redelivery after the work was already done.** worker-1 was started with `--crash-after-work`. It processed `order-0005` and crashed before sending the ACK. RabbitMQ redelivered the message to worker-2, which detected the duplicate:

```
22:05:32.692 [worker-1 pid=2681] DONE      order_id=order-0005 processed
22:05:32.692 [worker-1 pid=2681] SIMULATED CRASH: work done but exiting before ACK for event_id=7c2fd731-1c71-40a9-a5cf-ebd77cc904a1 (redelivery will be a duplicate)
22:05:32.695 [worker-2 pid=2690] RECEIVED  event_id=7c2fd731-1c71-40a9-a5cf-ebd77cc904a1 order_id=order-0005 item_id=item-500 qty=3  [REDELIVERED]
22:05:32.696 [worker-2 pid=2690] DUPLICATE event_id=7c2fd731-1c71-40a9-a5cf-ebd77cc904a1 already processed -> skipping business action
22:05:32.697 [worker-2 pid=2690] ACK       event_id=7c2fd731-1c71-40a9-a5cf-ebd77cc904a1 (duplicate discarded)

order-0005 deliveries:          2
order-0005 business action ran: 1 time(s)
```

## Reflection

RabbitMQ guarantees at-least-once delivery: a message stays in the queue until it is acknowledged, and if a worker crashes or loses its connection before the broker receives the ACK, the broker redelivers the message to another consumer. The broker can't tell whether the worker failed before or after doing the work, so a redelivered message may already have been processed. A message can also be duplicated before it reaches the queue, for example when a producer retries a publish it isn't sure succeeded. Idempotent handling makes this safe: each event carries a unique `event_id`, and a worker that sees an `event_id` it has already processed skips the business action and just ACKs the message. As a result, redeliveries and duplicate publishes don't repeat side effects such as reserving stock or charging a customer twice.
