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

@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    logger = get_run_logger()  # Prefect's logger for this task run; also goes to our log file

    url, params = URL, {"per_page": PER_PAGE}  # params only needed on the first request
    all_commits = []                           # collects every commit across all pages

    # follow GitHub's "next" links until we've pulled PAGES pages or run out of pages
    for page in range(1, PAGES + 1):
        try:
            response = httpx.get(url, headers=HEADERS, params=params)
            response.raise_for_status()  # raise an error on 4xx/5xx so the task's retries kick in
        except httpx.HTTPError as e:
            logger.error(f"GitHub request failed on page {page}: {e}")
            raise  # re-raise so Prefect marks the task failed and retries it
        commits = response.json()
        all_commits.extend(commits)
        print(f"page {page}: {len(commits)} commits (total {len(all_commits)})")

        url = response.links.get("next", {}).get("url")  # None on the last page
        if not url:
            break
        params = None  # the "next" URL already contains per_page and page

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
    print(f"stored {conn.execute('SELECT count(*) FROM commits').fetchone()[0]} rows in commits")


# name the flow something you can identify as yourself
@flow(name="vra7dy-flow")
def main_flow():
    ingest_data()

# it doesn't seem this flow executes when you run it. Why?
# fix it ;^)

# only run the flow when this file is executed directly, not when imported
if __name__ == "__main__":
    main_flow()
