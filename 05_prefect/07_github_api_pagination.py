"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10

# copy every Prefect log line (including print() output from log_prints) into a local file
LOG_FILE = Path(__file__).with_suffix(".log")  # 05_prefect/07_github_api_pagination.log
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(name)s - %(message)s"))
logging.getLogger("prefect").addHandler(file_handler)


# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

# local duckdb file, created if missing
conn = duckdb.connect("actions.duckdb")  

# concurrent version: pages are requested by number instead of following "next" links,
# since each "next" link is only known after the previous page returns (inherently serial).
# the serial "next"-link version is in git history (commit 32b8dd2).
# visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

@task(name="fetch_page", log_prints=True, retries=3, retry_delay_seconds=5)
def fetch_page(page: int) -> list[dict]:
    # fetch one page by number; pages don't depend on each other, so they can all run at once
    logger = get_run_logger()  # Prefect's logger for this task run; also goes to our log file
    try:
        response = httpx.get(URL, headers=HEADERS, params={"per_page": PER_PAGE, "page": page}, timeout=30)
        response.raise_for_status()  # raise an error on 4xx/5xx so the task's retries kick in
    except httpx.HTTPError as e:
        logger.error(f"GitHub request failed on page {page}: {e}")
        raise  # re-raise so Prefect marks the task failed and retries it
    commits = response.json()
    # logger, not print(): log_prints' print-patching can race when tasks start on many threads at once
    logger.info(f"page {page}: {len(commits)} commits")
    return commits


@task(name="store", log_prints=True)
def store_commits(all_commits: list[dict]):
    # flatten the commits and bulk-load them into a single DuckDB table
    logger = get_run_logger()

    # keep only the fields we need, one tuple per commit (order matches the DataFrame columns below)
    rows = [
        (
            c["sha"],
            c["commit"]["author"]["name"],
            c["commit"]["author"]["date"],          # ISO string; DuckDB casts it to a timestamp
            c["commit"]["message"].splitlines()[0], # subject line only
            json.dumps(c),                          # full original record, in case we need more later
        )
        for c in all_commits
    ]

    # load all rows in one bulk statement instead of 1000 single-row INSERTs;
    # CREATE OR REPLACE rebuilds the table on every run so reruns give 1000 rows, not 2000
    df = pd.DataFrame(rows, columns=["sha", "author_name", "author_date", "message", "raw"])
    try:
        conn.register("commits_df", df)  # expose the DataFrame to SQL as a virtual table
        conn.execute("""
            CREATE OR REPLACE TABLE commits AS
            SELECT sha,
                   author_name,
                   author_date::TIMESTAMPTZ AS author_date,
                   message,
                   raw::JSON                AS raw,
                   current_timestamp        AS ingested_at
            FROM commits_df
        """)
    except duckdb.Error:
        logger.exception("failed to write commits to actions.duckdb")
        raise
    # count(DISTINCT sha) < count(*) would mean a push mid-run shifted a commit onto two pages
    total, unique = conn.execute("SELECT count(*), count(DISTINCT sha) FROM commits").fetchone()
    print(f"stored {total} rows ({unique} unique commits) in commits")


# name the flow something you can identify as yourself
# ThreadPoolTaskRunner gives the flow PAGES threads so every fetch_page can run at the same time
@flow(name="vra7dy-flow", task_runner=ThreadPoolTaskRunner(max_workers=PAGES))
def main_flow():
    futures = fetch_page.map(range(1, PAGES + 1))      # fan out: one task per page, all submitted at once
    pages = futures.result()                           # fan in: wait for all 10; a list of 10 lists
    all_commits = [c for page in pages for c in page]  # flatten into one list of 1000 commits
    store_commits(all_commits)

# it doesn't seem this flow executes when you run it. Why?
# fix it ;^)

# only run the flow when this file is executed directly, not when imported
if __name__ == "__main__":
    main_flow()
