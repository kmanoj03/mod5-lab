#!/usr/bin/env bash
# Runs the three lab scenarios end-to-end and writes the logs to evidence/.
#
#   ./scripts/run_demos.sh            # all demos
#   ./scripts/run_demos.sh 2          # only demo 2
#
# Requires a running broker. With Docker set:
#   RABBITMQCTL="docker exec mod5-rabbitmq rabbitmqctl" ./scripts/run_demos.sh
set -euo pipefail

cd "$(dirname "$0")/.."
PY="${PY:-.venv/bin/python}"
RABBITMQCTL="${RABBITMQCTL:-rabbitmqctl}"
QUEUE="${QUEUE_NAME:-orders}"
OUT=evidence
TMP=$(mktemp -d)
mkdir -p "$OUT"

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

queue_stats() {
  $RABBITMQCTL -q list_queues name messages_ready messages_unacknowledged consumers \
    | awk -v q="$QUEUE" 'NR==1 || $1==q'
}

reset_state() {
  $RABBITMQCTL -q purge_queue "$QUEUE" >/dev/null 2>&1 || true
  rm -rf processed_events.txt processed_events.txt.locks "$TMP"/*.log
}

start_worker() {  # start_worker <name> [extra args...]
  local name=$1; shift
  "$PY" -u worker.py --name "$name" "$@" >"$TMP/$name.log" 2>&1 &
  PIDS+=($!)
  echo $!
}

wait_until_drained() {  # no ready and no unacked messages left
  for _ in $(seq 1 60); do
    read -r ready unacked < <(queue_stats | awk -v q="$QUEUE" '$1==q {print $2, $3}') || true
    [ "${ready:-1}" = "0" ] && [ "${unacked:-1}" = "0" ] && return 0
    sleep 1
  done
  echo "queue did not drain in time" >&2; return 1
}

stop_workers() { cleanup; wait 2>/dev/null || true; PIDS=(); }

merged_timeline() { sort -s -k1,1 "$TMP"/*.log; }

section() { printf '\n===== %s =====\n' "$1"; }

# --------------------------------------------------------------------------
demo1() {
  local f="$OUT/01_competing_consumers.log"
  reset_state
  {
    echo "# Demo 1: successful processing + two competing consumers"
    echo "# worker-1 and worker-2 consume the same queue '$QUEUE' (prefetch=1, manual ACK)."
    echo "# The producer publishes 10 orders; each order must be handled by exactly one worker."
    start_worker worker-1 --delay 1 >/dev/null
    start_worker worker-2 --delay 1 >/dev/null
    sleep 2
    section "queue before publishing"; queue_stats
    "$PY" -u producer.py --count 10 >"$TMP/producer.log" 2>&1
    wait_until_drained
    sleep 1; stop_workers
    section "merged timeline (producer + both workers)"; merged_timeline
    section "summary"
    for w in worker-1 worker-2; do
      printf '%s processed %s orders: %s\n' "$w" \
        "$(grep -c 'DONE' "$TMP/$w.log")" \
        "$(grep 'DONE' "$TMP/$w.log" | awk '{print $5}' | sed 's/order_id=//' | paste -sd' ' -)"
    done
    echo "orders published:          $(grep -c 'published {' "$TMP/producer.log")"
    echo "orders processed (total):  $(cat "$TMP"/worker-*.log | grep -c 'DONE')"
    echo "distinct order_ids done:   $(cat "$TMP"/worker-*.log | grep 'DONE' | awk '{print $5}' | sort -u | wc -l | tr -d ' ')"
    section "queue after processing"; queue_stats
  } 2>&1 | tee "$f"
}

# --------------------------------------------------------------------------
demo2() {
  local f="$OUT/02_failure_redelivery.log"
  reset_state
  {
    echo "# Demo 2: worker dies after receiving a message but before ACK"
    echo "# worker-1 is the only consumer when 3 orders are published, so it receives order-0001."
    echo "# It is started with --crash-before-ack and exits mid-processing without ACKing."
    echo "# worker-2 is started right after; it should receive order-0001 again (redelivered=True)."
    start_worker worker-1 --delay 4 --crash-before-ack >/dev/null
    sleep 2
    "$PY" -u producer.py --count 3 >"$TMP/producer.log" 2>&1
    sleep 0.5
    section "queue while worker-1 holds order-0001 unacknowledged"; queue_stats
    start_worker worker-2 --delay 1 >/dev/null
    sleep 1.2
    section "queue just after worker-1 crashed (consumers back to 1, order-0001 now owned by worker-2)"; queue_stats
    wait_until_drained

    echo
    echo "# System keeps running: restart worker-1 normally and publish order-0004..0007."
    start_worker worker-1-restarted --delay 1 >/dev/null
    sleep 2
    "$PY" -u producer.py --count 4 --start 4 >>"$TMP/producer.log" 2>&1
    wait_until_drained
    sleep 1; stop_workers

    section "merged timeline"; merged_timeline
    section "summary"
    echo "worker-1 received:               $(grep 'RECEIVED' "$TMP/worker-1.log" | awk '{print $6}' | sed 's/order_id=//')"
    echo "worker-1 ACKs sent:              $(grep -c '\] ACK ' "$TMP/worker-1.log" || true)"
    echo "worker-2 got as redelivery:      $(grep 'REDELIVERED' "$TMP/worker-2.log" | awk '{print $6}' | sed 's/order_id=//')"
    echo "order-0001 business action ran:  $(cat "$TMP"/worker*.log | grep -c 'DONE      order_id=order-0001 ') time(s)"
    echo "orders processed after restart:  $(grep -h 'DONE' "$TMP"/worker*.log | awk '{print $2, $5}' | sed 's/order_id=//' | grep -E 'order-000[4-7]' | tr -d '[' | paste -sd',' - | sed 's/,/, /g')"
    echo "processed event_ids recorded:    $(wc -l < processed_events.txt | tr -d ' ') (expected 7)"
    section "queue at the end"; queue_stats
  } 2>&1 | tee "$f"
}

# --------------------------------------------------------------------------
demo3() {
  local f="$OUT/03_duplicate_handling.log"
  reset_state
  {
    echo "# Demo 3: duplicate handling (shared processed_events.txt, keyed by event_id)"
    echo
    echo "## Part A: the producer publishes order-0001..0004, then re-publishes order-0001"
    echo "## with the SAME event_id. The copy must be detected and the business action skipped."
    start_worker worker-1 --delay 1 >/dev/null
    start_worker worker-2 --delay 1 >/dev/null
    sleep 2
    "$PY" -u producer.py --count 4 --duplicate >"$TMP/producer.log" 2>&1
    wait_until_drained
    sleep 1; stop_workers
    section "Part A merged timeline"; merged_timeline
    section "Part A summary"
    echo "messages published:            $(grep -c '\] published' "$TMP/producer.log")"
    echo "business actions executed:     $(cat "$TMP"/worker-*.log | grep -c 'DONE')"
    echo "duplicates detected/skipped:   $(cat "$TMP"/worker-*.log | grep -c 'DUPLICATE')"
    section "processed_events.txt after Part A"; cat processed_events.txt

    rm -f "$TMP"/*.log
    echo
    echo "## Part B: the real-world cause of duplicates. worker-1 (sole consumer) processes"
    echo "## order-0005 and records its event_id, then crashes BEFORE the ACK reaches the broker."
    echo "## RabbitMQ redelivers order-0005 to worker-2, which must detect it as a duplicate."
    start_worker worker-1 --delay 1 --crash-after-work >/dev/null
    sleep 2
    "$PY" -u producer.py --count 1 --start 5 >"$TMP/producer.log" 2>&1
    sleep 0.3
    start_worker worker-2 --delay 1 >/dev/null
    wait_until_drained
    sleep 1; stop_workers
    section "Part B merged timeline"; merged_timeline
    section "Part B summary"
    echo "order-0005 deliveries:          $(cat "$TMP"/worker-*.log | grep -c 'RECEIVED .*order-0005')"
    echo "order-0005 business action ran: $(cat "$TMP"/worker-*.log | grep -c 'DONE      order_id=order-0005 ') time(s)"
    echo "duplicate detected by:          $(grep -l 'DUPLICATE' "$TMP"/worker-*.log | xargs -n1 basename | sed 's/.log$//')"
    section "processed_events.txt after Part B"; cat processed_events.txt
  } 2>&1 | tee "$f"
}

case "${1:-all}" in
  1) demo1 ;;
  2) demo2 ;;
  3) demo3 ;;
  all) demo1; demo2; demo3 ;;
  *) echo "usage: $0 [1|2|3|all]"; exit 1 ;;
esac
