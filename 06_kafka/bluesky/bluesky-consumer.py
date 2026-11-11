from collections import Counter
import json
import time
import redis
from quixstreams import Application

"""
Consume Bluesky events from the `bluesky-events` topic and keep a running count of each
commit operation (create/update/delete) in a single Redis hash.

Counts are batched in memory and written to Redis once per FLUSH_INTERVAL seconds (or every
FLUSH_MAX_MSGS messages) in one pipeline. Kafka offsets are committed only AFTER the Redis
write succeeds (at-least-once: a crash between the two can double-count one batch).
On Ctrl-C, pending counts are flushed and the totals are printed.
"""

KAFKA_BROKER = "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092"
TOPIC = "bluesky-events"
REDIS_KEY = "bluesky:operations"   # One hash holds all counts for this project
FLUSH_INTERVAL = 1                 # Seconds between Redis writes
FLUSH_MAX_MSGS = 1000              # ...or flush early once this many messages are pending

redis_client = redis.Redis(host="localhost", port=6379, db=0)


def flush(consumer, pending):
    """Write pending counts to Redis in one transaction, then commit Kafka offsets."""
    if not pending:
        return
    pipe = redis_client.pipeline()   # MULTI/EXEC: all increments apply together or not at all
    for operation, n in pending.items():
        pipe.hincrby(REDIS_KEY, operation, n)
    pipe.execute()
    # Commit only after Redis has the counts; sends the offsets stored for every message polled so far
    consumer.commit(asynchronous=False)
    print(f"+{sum(pending.values()):,} msgs ({dict(pending)})")
    pending.clear()


def print_totals():
    """Print the running totals stored in this project's Redis hash."""
    totals = redis_client.hgetall(REDIS_KEY)
    print(f"\nTotals in Redis ({REDIS_KEY}):")
    for op, count in sorted(totals.items(), key=lambda kv: -int(kv[1])):
        print(f"  {op.decode('utf8'):<8} {int(count):,}")


def main():
    app = Application(
        broker_address=KAFKA_BROKER,
        consumer_group="kafka-stream-counter",
        auto_offset_reset="earliest",
        consumer_extra_config={
            # Resolve localhost to 127.0.0.1 to avoid IPv6 issues
            "broker.address.family": "v4",
        }
    )

    pending = Counter()        # Operation counts not yet written to Redis
    last_flush = time.time()

    # Auto-commit off: offsets are committed manually after each Redis flush
    with app.get_consumer(auto_commit_enable=False) as consumer:
        consumer.subscribe([TOPIC])
        try:
            while True:
                msg = consumer.poll(1)

                if msg is not None:
                    if msg.error() is not None:
                        raise Exception(msg.error())
                    try:
                        value = json.loads(msg.value())
                        pending[value["commit"]["operation"]] += 1
                    except (json.JSONDecodeError, KeyError, TypeError) as e:
                        print(f"Skipping bad message at offset {msg.offset()} ({type(e).__name__}: {e})")
                    # Mark the offset as processed (local only); it's sent to Kafka on the next commit
                    consumer.store_offsets(msg)

                # Flush on time or size, whichever comes first (also runs while the topic is idle)
                if time.time() - last_flush >= FLUSH_INTERVAL or sum(pending.values()) >= FLUSH_MAX_MSGS:
                    flush(consumer, pending)
                    last_flush = time.time()
        finally:
            # Runs on Ctrl-C too: don't lose the last partial batch
            flush(consumer, pending)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    except redis.exceptions.ConnectionError as e:
        print(f"Redis connection failed: {e}")
    finally:
        try:
            print_totals()
        except redis.exceptions.ConnectionError:
            print("Could not read totals: Redis unavailable.")
        redis_client.close()
