import gzip
import json
from collections import Counter
import requests
from prefect import flow, task, get_run_logger


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"


@task(retries=3)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    # use a Counter named "counts" and pull out the "type"
    # for each line in the stream
    logger = get_run_logger()
    counts = Counter()
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
            r.raise_for_status()  # fail on 404/5xx so Prefect retries
            # decompress the gzip stream on the fly without saving to disk
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for line in gz:
                    try:
                        counts[json.loads(line)["type"]] += 1
                    except (json.JSONDecodeError, KeyError):
                        # skip lines that are malformed or have no "type"
                        logger.warning(f"hour {hour}: skipped bad line")
    except requests.RequestException as e:
        logger.error(f"hour {hour}: request failed: {e}")
        raise  # re-raise so retries=3 kicks in
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    pass


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
