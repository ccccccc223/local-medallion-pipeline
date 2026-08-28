"""
Bronze layer: land the raw CSV batches in SQLite with no transformation.

The Bronze layer's only job is faithful capture. Values are stored exactly
as they appear in the source files, as text, so nothing is lost or altered
before it has been inspected. Cleaning, casting and deduplication belong to
Silver.

Three lineage columns are added to every row so any record can be traced
back to the file it arrived in:
    source_file      the CSV filename
    source_row       the row's position within that file
    batch_timestamp  the batch time parsed out of that filename
    ingested_at      when this pipeline run loaded it

source_row exists so that downstream layers have a deterministic tiebreak
that does not depend on the order SQLite happens to return rows in. SQL does
not guarantee row order without an ORDER BY, so anything ordered only by
business columns is deterministic by accident rather than by construction.

Run:  python src/load_bronze.py
"""

import glob
import os
import sqlite3
from datetime import datetime

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DB_DIR = os.path.join(PROJECT_ROOT, "database")
DB_PATH = os.path.join(DB_DIR, "analytics.db")

# Source folder name -> Bronze table name.
DATASETS = {
    "agent": "bronze_agent",
    "agent-commission": "bronze_agent_commission",
    "location": "bronze_location",
    "orders": "bronze_orders",
    "sales": "bronze_sales",
}


def parse_batch_timestamp(filename):
    """Recover the batch time from a filename like 20250417-124118559367-sales.csv.

    The first part is the date and the second is the time of day to
    microsecond precision. Two batches of the same dataset arrive at
    different times, so this is what distinguishes them once the files
    have been concatenated.
    """
    date_part, time_part = filename.split("-")[0], filename.split("-")[1]
    stamp = datetime.strptime(date_part + time_part, "%Y%m%d%H%M%S%f")
    return stamp.isoformat()


def read_batches(dataset_name, ingested_at):
    """Read every batch file for one dataset and stack them, adding lineage."""
    paths = sorted(glob.glob(os.path.join(RAW_DIR, f"*-{dataset_name}.csv")))
    if not paths:
        raise FileNotFoundError(
            f"No files matching *-{dataset_name}.csv in {RAW_DIR}"
        )

    batches = []
    for path in paths:
        filename = os.path.basename(path)
        # dtype=str keeps every value exactly as written, including the
        # leading zeros in Sales that type inference would otherwise strip.
        batch = pd.read_csv(path, dtype=str)
        batch["source_file"] = filename
        batch["source_row"] = range(1, len(batch) + 1)
        batch["batch_timestamp"] = parse_batch_timestamp(filename)
        batch["ingested_at"] = ingested_at
        batches.append(batch)
        print(f"    {filename}: {len(batch)} rows")

    return pd.concat(batches, ignore_index=True)


def count_source_rows(dataset_name):
    """Count data rows in the source files, excluding header lines.

    This is deliberately independent of pandas so that the row-count check
    below is a real check rather than a comparison of pandas against itself.
    """
    total = 0
    for path in sorted(glob.glob(os.path.join(RAW_DIR, f"*-{dataset_name}.csv"))):
        with open(path, encoding="utf-8") as handle:
            total += sum(1 for _ in handle) - 1
    return total


def main():
    os.makedirs(DB_DIR, exist_ok=True)
    ingested_at = datetime.now().isoformat(timespec="seconds")

    connection = sqlite3.connect(DB_PATH)
    try:
        for dataset_name in DATASETS:
            table_name = DATASETS[dataset_name]
            print(f"\n{dataset_name} -> {table_name}")

            frame = read_batches(dataset_name, ingested_at)

            # replace, not append, so re-running the pipeline is idempotent
            # and always reproduces the database from the source files.
            frame.to_sql(table_name, connection, if_exists="replace", index=False)

            # Bronze must not lose or invent rows. Compare what landed in the
            # database against an independent count of the source files.
            expected = count_source_rows(dataset_name)
            loaded = pd.read_sql(f"SELECT COUNT(*) AS n FROM {table_name}",
                                 connection)["n"][0]
            if loaded != expected:
                raise AssertionError(
                    f"{table_name}: loaded {loaded} rows, source has {expected}"
                )
            print(f"    loaded {loaded} rows, matches source")

        connection.commit()
    finally:
        connection.close()

    print(f"\nBronze layer written to {DB_PATH}")


if __name__ == "__main__":
    main()