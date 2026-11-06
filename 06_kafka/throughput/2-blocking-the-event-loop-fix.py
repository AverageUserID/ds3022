"""
FIX 2: Move blocking work off the event loop and run it concurrently.

What changed (vs. 2-blocking-the-event-loop.py):
  - The reader coroutine ONLY reads the socket, parses, and enqueues. It never blocks,
    so keepalive pings are answered and the socket is drained.
  - A bounded asyncio.Queue decouples reading from enrichment. If workers fall behind,
    `await queue.put()` pauses the reader: explicit, bounded backpressure.
  - A fixed pool of WORKERS coroutines runs the sync enrich() in a thread pool via
    asyncio.to_thread(), so up to WORKERS lookups are in flight at once.
    Needed concurrency ~= rate x latency = 1000 msg/s x 5 ms = 5; WORKERS=16 gives headroom.
  - produce() + poll(0) stay on the loop thread (no per-message flush; see fix 1).

Trade-off: with several workers, messages can be produced slightly out of order.
Watch:  `lag` should stay near 0 and `missed` at 0.
"""
import asyncio
import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
import websockets
from quixstreams import Application

URL = "ws://127.0.0.1:8765/"
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "127.0.0.1:19092,127.0.0.1:29092,127.0.0.1:39092")
WORKERS = 16        # concurrent enrichments (threads + worker coroutines)
QUEUE_SIZE = 1000   # bounded buffer between reader and workers (~1 s at 1000 msg/s)

# Log to a local file and the console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler("2-blocking-the-event-loop-fix.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

app = Application(broker_address=KAFKA_BROKER, producer_extra_config={"broker.address.family": "v4"})
topic = app.topic(name="trades", value_serializer="json")
stats = {"count": 0, "missed": 0}  # shared by coroutines; safe because they all run on one thread


def enrich(data):
    """Stand-in for a synchronous lookup, e.g. requests.get("https://geo.example.com/...")."""
    time.sleep(0.005)                                   # still blocking, but now runs in a worker thread
    data["enriched"] = True
    return data


async def reader(ws, queue):
    """Drain the socket as fast as possible: parse, track seq gaps, enqueue."""
    last_seq = None
    async for message in ws:
        data = json.loads(message)
        # Gap tracking happens here, in arrival order, before workers can reorder anything
        if last_seq is not None and data["seq"] != last_seq + 1:
            stats["missed"] += data["seq"] - last_seq - 1
        last_seq = data["seq"]
        await queue.put(data)                           # waits only if the queue is full (backpressure)


async def worker(queue, producer):
    """Pull messages, enrich in a thread (loop stays free), then produce to Kafka."""
    while True:
        data = await queue.get()
        try:
            data = await asyncio.to_thread(enrich, data)   # <-- THE FIX (blocking call off the loop)
            msg = topic.serialize(key=data["symbol"], value=data)
            producer.produce(topic=topic.name, key=msg.key, value=msg.value)
            producer.poll(0)
            stats["count"] += 1
            if stats["count"] % 1000 == 0:
                log.info(f"produced={stats['count']} missed={stats['missed']} "
                         f"lag={time.time() - data['ts']:.2f}s qsize={queue.qsize()}")
        except Exception as e:
            log.error(f"Failed to process seq={data.get('seq')}: {e!r}")
        finally:
            queue.task_done()


async def main():
    """Wire up reader -> bounded queue -> worker pool -> Kafka producer."""
    # Size the default executor so asyncio.to_thread has WORKERS threads available
    asyncio.get_running_loop().set_default_executor(ThreadPoolExecutor(max_workers=WORKERS))
    queue = asyncio.Queue(maxsize=QUEUE_SIZE)

    with app.get_producer() as producer:
        workers = [asyncio.create_task(worker(queue, producer)) for _ in range(WORKERS)]
        try:
            async with websockets.connect(URL) as ws:
                await reader(ws, queue)
        except Exception as e:
            log.error(f"Connection lost: {e!r}")
        finally:
            # Finish what's already queued, stop workers, then deliver anything buffered
            await queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            remaining = producer.flush(timeout=10)
            if remaining:
                log.warning(f"{remaining} messages not delivered before shutdown")
    log.info(f"Total produced: {stats['count']}, missed: {stats['missed']}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Stopped by user")
