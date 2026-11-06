"""
FIX 1: Let the Kafka client batch instead of flushing after every message.

What changed (vs. 1-flushing-every-message.py):
  - No flush() in the hot loop. produce() just buffers; librdkafka batches and sends
    in the background.
  - producer.poll(0) after each produce() serves delivery callbacks and frees buffer
    space without blocking.
  - Optional periodic flush (FLUSH_EVERY) as a durability checkpoint, not per message.
  - Batching tuned via linger.ms / batch.size / compression.
  - One final flush on shutdown so every buffered message is still delivered.

Watch:  `lag` should stay near 0 and `missed` at 0.
"""
import asyncio
import json
import logging
import os
import time
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
FLUSH_EVERY = 5000  # checkpoint flush interval (messages); set to 0 to disable

# Log to a local file and the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("1-flushing-every-message-fix.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# Batching config: wait up to 5 ms to fill larger, compressed batches
app = Application(
    broker_address=KAFKA_BROKER,
    producer_extra_config={
        "broker.address.family": "v4",
        "linger.ms": 5,
        "batch.size": 131072,
        "compression.type": "lz4",
    },
)
topic = app.topic(name="trades", value_serializer="json")


async def main():
    """Read trades from the WebSocket and produce each one to Kafka without per-message flushes."""
    count, last_seq, missed = 0, None, 0
    with app.get_producer() as producer:
        try:
            async with websockets.connect(URL) as ws:
                async for message in ws:
                    data = json.loads(message)
                    msg = topic.serialize(key=data["symbol"], value=data)
                    producer.produce(topic=topic.name, key=msg.key, value=msg.value)
                    producer.poll(0)                    # <-- THE FIX (non-blocking; no per-message ack wait)

                    # Bookkeeping: count gaps in `seq` and how far behind real time we are
                    if last_seq is not None and data["seq"] != last_seq + 1:
                        missed += data["seq"] - last_seq - 1
                    last_seq, count = data["seq"], count + 1

                    # Occasional checkpoint flush instead of one per message
                    if FLUSH_EVERY and count % FLUSH_EVERY == 0:
                        producer.flush(timeout=5)
                    if count % 1000 == 0:
                        log.info(f"produced={count} missed={missed} lag={time.time() - data['ts']:.2f}s")
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
        finally:
            # Deliver anything still buffered before the producer closes
            remaining = producer.flush(timeout=10)
            if remaining:
                log.warning(f"{remaining} messages not delivered before shutdown")
    log.info(f"Total produced: {count}, missed: {missed}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
