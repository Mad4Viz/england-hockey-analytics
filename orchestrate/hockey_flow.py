"""
England Hockey Analytics - Prefect Orchestration

Wraps the existing scrape -> load -> dbt pipeline into one automated flow.

Before Prefect, each step was run manually in sequence:
    1. python -m extract --type all --headless
    2. bq load ... (3 separate commands)
    3. dbt build --target dev
    4. (check results, then manually) dbt build --target prod

This flow automates all 4 steps with:
    - Blue-green data staging: scraper writes to pre_production, not production
    - Dev gate: prod only runs if dev passes all tests
    - Timestamped backups: production is backed up before each promotion
    - Retry logic: scraper retries 2x if the website is flaky
    - Checkpoints: logs row counts before/after each step

Data directory layout:
    data/sample/          GitHub showcase (NEVER touched by this flow)
    data/production/      Last known good CSVs (only updated after full success)
    data/pre_production/  Working area (wiped + rebuilt each run)
    data/backups/         Timestamped snapshots before each promotion

Requirements:
    - .env file with GOOGLE_APPLICATION_CREDENTIALS (see .env.example)
    - Chrome/ChromeDriver installed (for Selenium scraper)
    - All pip dependencies installed (see requirements.txt)

Usage:
    python orchestrate/hockey_flow.py                  # incremental refresh
    python orchestrate/hockey_flow.py --full-refresh    # re-scrape everything
"""

from prefect import flow, task
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
import subprocess
import shutil
import csv
import os
import sys
import argparse

# Load credentials from .env (gitignored — keeps key paths out of source code)
load_dotenv(Path(__file__).parent.parent / ".env")

# ---------------------------------------------------------------------------
# PATHS — resolved relative to this file so it works from any working directory
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
DBT_DIR = PROJECT_ROOT / "transform" / "hockey_dbt"

# Data directories
PRODUCTION_DIR = PROJECT_ROOT / "data" / "production"
PRE_PRODUCTION_DIR = PROJECT_ROOT / "data" / "pre_production"
BACKUPS_DIR = PROJECT_ROOT / "data" / "backups"

# The three CSV tables that move through the pipeline
TABLES = ["standings", "matches", "match_events"]

# BigQuery destination for raw data
BQ_PROJECT = "england-hockey-analytics"
BQ_DATASET = "hockey_raw"


# =============================================================================
# HELPERS
# =============================================================================

def count_csv_rows(csv_path: Path) -> int:
    """Count data rows in a CSV (excludes header). Returns 0 if file missing."""
    if not csv_path.exists():
        return 0
    with open(csv_path, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1


def checkpoint(label: str, details: dict):
    """
    Print a formatted checkpoint summary.

    WHY: When the flow runs (locally or on Prefect Cloud), these checkpoints
    appear in the logs so you can see exactly what happened at each stage
    without digging through verbose output.
    """
    print(f"\n{'='*60}")
    print(f"  CHECKPOINT: {label}")
    print(f"{'='*60}")
    for key, value in details.items():
        print(f"  {key}: {value}")
    print(f"{'='*60}\n")


def row_counts(directory: Path) -> dict:
    """Get row counts for all tables in a directory."""
    return {t: count_csv_rows(directory / f"{t}.csv") for t in TABLES}


def get_last_scrape_date(production_dir: Path) -> str:
    """Find the most recent scraped_at date from production matches CSV.

    Returns YYYY-MM-DD string, or None if no production data exists.
    """
    matches_csv = production_dir / "matches.csv"
    if not matches_csv.exists():
        return None

    max_date = None
    with open(matches_csv, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            scraped = row.get("scraped_at", "")[:10]  # "2026-02-03T14:30:00" → "2026-02-03"
            if scraped and scraped > (max_date or ""):
                max_date = scraped
    return max_date


# =============================================================================
# TASK 1: PREPARE WORKING COPY
# =============================================================================
# WHY: We never scrape directly into production. Instead, we copy production
# into pre_production (so the scraper starts with full history), then the
# scraper's upsert logic merges new rows into that copy. If anything fails
# later, production is untouched.

@task(name="prepare-working-copy")
def prepare_working_copy() -> dict:
    """
    Back up production, then wipe pre_production and copy production into it.

    Safety backup first: even if the flow crashes mid-run, production data
    is preserved in data/backups/YYYY-MM-DD_HH-MM/.

    Returns row counts of the working copy.
    """
    # --- Step 1: Safety backup of production before doing anything ---
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    backup_dir = BACKUPS_DIR / timestamp

    production_counts = row_counts(PRODUCTION_DIR)

    if any(production_counts.values()):
        backup_dir.mkdir(parents=True, exist_ok=True)
        for t in TABLES:
            src = PRODUCTION_DIR / f"{t}.csv"
            if src.exists():
                shutil.copy2(src, backup_dir / f"{t}.csv")

        checkpoint("Production Backed Up", {
            "backup_path": str(backup_dir),
            **{t: f"{n} rows" for t, n in production_counts.items()},
        })
    else:
        checkpoint("Production Backup Skipped", {
            "reason": "No existing production data to back up",
        })

    # --- Step 2: Wipe pre_production clean ---
    if PRE_PRODUCTION_DIR.exists():
        shutil.rmtree(PRE_PRODUCTION_DIR)
    PRE_PRODUCTION_DIR.mkdir(parents=True, exist_ok=True)

    # --- Step 3: Copy production CSVs into pre_production ---
    copied = {}
    for t in TABLES:
        src = PRODUCTION_DIR / f"{t}.csv"
        dst = PRE_PRODUCTION_DIR / f"{t}.csv"
        if src.exists():
            shutil.copy2(src, dst)
            copied[t] = count_csv_rows(dst)
        else:
            copied[t] = 0

    checkpoint("Working Copy Prepared", {
        t: f"{n} rows copied from production" for t, n in copied.items()
    })
    return copied


# =============================================================================
# TASK 2: SCRAPE
# =============================================================================
# WHY retries=2: The England Hockey website can be flaky (timeouts, slow loads).
# The scraper uses upsert writers that save incrementally, so a retry picks up
# where it left off — it won't re-scrape data it already saved.

@task(name="scrape-hockey-data", retries=2, retry_delay_seconds=60)
def scrape(headless: bool = True, full_refresh: bool = False) -> dict:
    """
    Run the Selenium scraper to pull latest data from England Hockey website.

    The HOCKEY_OUTPUT_DIR env var redirects the scraper to write into
    pre_production instead of data/sample (which is the GitHub showcase).

    Incremental by default: calculates --since from production CSV so only
    match days after the last scrape are visited. Use full_refresh=True to
    scrape the entire season.

    Returns dict with before/after row counts per table.
    """
    # --- Calculate --since date from production data ---
    last_scrape = get_last_scrape_date(PRODUCTION_DIR)
    today = datetime.now().strftime("%Y-%m-%d")

    if full_refresh or not last_scrape:
        since_date = None
        mode_detail = "Full season (no --since)"
    else:
        since_date = last_scrape
        mode_detail = f"--since {since_date}"

    checkpoint("Scrape Window", {
        "last_scrape": last_scrape or "None (first run)",
        "today": today,
        "mode": mode_detail,
    })

    # --- Checkpoint: snapshot row counts BEFORE scraping ---
    before = row_counts(PRE_PRODUCTION_DIR)
    checkpoint("Before Scrape", {t: f"{n} rows" for t, n in before.items()})

    # --- Build the scrape command ---
    cmd = [
        sys.executable, "-m", "extract",
        "--type", "all",
        "--division", "both",
        "--season", "current",
    ]
    if headless:
        cmd.append("--headless")
    if full_refresh:
        cmd.append("--full-refresh")
    if since_date:
        cmd.extend(["--since", since_date])

    # Set HOCKEY_OUTPUT_DIR so the scraper writes to pre_production
    scraper_env = {**os.environ, "HOCKEY_OUTPUT_DIR": str(PRE_PRODUCTION_DIR)}

    print(f"Running: {' '.join(cmd)}")
    print(f"Output dir: {PRE_PRODUCTION_DIR}")

    # Stream output in real-time so you can watch scraper progress
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=scraper_env,
    )

    if result.returncode != 0:
        raise Exception(f"Scraper failed (exit code {result.returncode})")

    # --- Checkpoint: snapshot row counts AFTER scraping ---
    after = row_counts(PRE_PRODUCTION_DIR)

    summary = {}
    for t in TABLES:
        diff = after[t] - before[t]
        sign = f"+{diff}" if diff >= 0 else str(diff)
        summary[t] = f"{before[t]} -> {after[t]} ({sign} rows)"

    checkpoint("After Scrape", summary)
    return {"before": before, "after": after}


# =============================================================================
# TASK 3: LOAD TO BIGQUERY
# =============================================================================
# WHY Python SDK instead of bq CLI: gives us the exact row count from the load
# job result, better error messages, and no dependency on gcloud CLI being
# installed. The google-cloud-bigquery package is already in requirements.txt.
#
# WHY WRITE_TRUNCATE is safe: pre_production contains the FULL history (copied
# from production) plus any new rows the scraper added. So replacing BigQuery
# tables with the full CSV content is correct.

@task(name="load-to-bigquery", retries=1, retry_delay_seconds=30)
def load_to_bigquery() -> dict:
    """
    Upload pre_production CSVs to BigQuery hockey_raw dataset.

    Uses WRITE_TRUNCATE so raw tables always reflect complete CSV content.
    This is safe because pre_production has the full history + new data.

    Returns dict with rows loaded per table.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    loaded = {}
    for table_name in TABLES:
        csv_path = PRE_PRODUCTION_DIR / f"{table_name}.csv"
        if not csv_path.exists():
            print(f"  WARNING: {csv_path.name} not found, skipping")
            loaded[table_name] = 0
            continue

        table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

        with open(csv_path, "rb") as f:
            job = client.load_table_from_file(f, table_id, job_config=job_config)
            job.result()  # Blocks until complete

        rows = job.output_rows
        loaded[table_name] = rows
        print(f"  Loaded {table_name}: {rows} rows -> {table_id}")

    checkpoint("After Load to BigQuery", {
        t: f"{n} rows loaded to {BQ_DATASET}.{t}" for t, n in loaded.items()
    })
    return loaded


# =============================================================================
# TASK 4: DBT BUILD — DEV
# =============================================================================
# WHY dev first: This is the safety gate. dbt build runs all models AND all
# schema tests (19+ tests). If any test fails, it means the data has an issue
# (nulls where there shouldn't be, duplicate keys, etc.). By running dev first,
# we catch problems BEFORE they reach prod (which Tableau connects to).
#
# WHY no retries: If dbt fails, it's a data quality issue, not a transient
# network error. Retrying the same bad data won't help.

@task(name="dbt-build-dev")
def dbt_build_dev() -> bool:
    """
    Run dbt build against the DEV target (hockey_dev dataset).

    This builds all models + runs all tests. If any test fails,
    raises an exception which stops the flow before prod runs.

    Returns True if passed.
    """
    print("Running: dbt build --target dev")
    result = subprocess.run(
        ["dbt", "build", "--target", "dev"],
        cwd=str(DBT_DIR),
    )

    passed = result.returncode == 0

    checkpoint("After dbt build (DEV)", {
        "target": "hockey_dev",
        "status": "PASSED" if passed else "FAILED",
        "action": "Proceeding to prod" if passed else "STOPPING - prod will NOT run",
    })

    if not passed:
        raise Exception(
            "dbt build failed on dev target. "
            "Production CSVs are untouched. Check test failures above."
        )

    return passed


# =============================================================================
# TASK 5: DBT BUILD — PROD
# =============================================================================
# WHY this is separate from dev: Prefect only calls this task if dbt_build_dev
# succeeded. This is the "promotion" step — the data has been validated in dev
# and is now safe to push to prod where Tableau reads from it.

@task(name="dbt-build-prod")
def dbt_build_prod() -> bool:
    """
    Run dbt build against the PROD target (hockey_prod dataset).

    Only called after dev passes. Updates the tables that Tableau connects to.

    Returns True if passed.
    """
    print("Running: dbt build --target prod")
    result = subprocess.run(
        ["dbt", "build", "--target", "prod"],
        cwd=str(DBT_DIR),
    )

    passed = result.returncode == 0

    checkpoint("After dbt build (PROD)", {
        "target": "hockey_prod",
        "status": "PASSED" if passed else "FAILED",
    })

    if not passed:
        raise Exception("dbt build failed on prod target. Check output above.")

    return passed


# =============================================================================
# TASK 6: PROMOTE TO PRODUCTION
# =============================================================================
# WHY this is a separate task: Only runs after EVERYTHING passes (scrape, load,
# dbt dev, dbt prod). Replaces production with the validated pre_production data.
# Production was already backed up at the start of the flow (in prepare_working_copy).

@task(name="promote-to-production")
def promote_to_production() -> dict:
    """
    Replace production with pre_production data.

    Safe because production was already backed up at the start of the flow.

    Returns dict with before/after row counts.
    """
    before_counts = row_counts(PRODUCTION_DIR)

    # Promote pre_production -> production
    for t in TABLES:
        src = PRE_PRODUCTION_DIR / f"{t}.csv"
        dst = PRODUCTION_DIR / f"{t}.csv"
        if src.exists():
            shutil.copy2(src, dst)

    after_counts = row_counts(PRODUCTION_DIR)

    summary = {}
    for t in TABLES:
        diff = after_counts[t] - before_counts[t]
        sign = f"+{diff}" if diff >= 0 else str(diff)
        summary[t] = f"{before_counts[t]} -> {after_counts[t]} ({sign} rows)"

    checkpoint("Promoted to Production", summary)
    return {"counts": after_counts}


# =============================================================================
# FLOW — ties everything together
# =============================================================================

@flow(name="hockey-weekly-refresh", log_prints=True)
def hockey_pipeline(full_refresh: bool = False):
    """
    Full pipeline: prepare -> scrape -> load -> dbt dev -> dbt prod -> promote

    The sequential order matters:
        1. Prepare: backup production, then copy it -> pre_production
        2. Scrape: upsert new data into pre_production CSVs
        3. Load: push pre_production CSVs -> BigQuery hockey_raw
        4. dbt dev: transform + test in hockey_dev (GATE)
        5. dbt prod: promote to hockey_prod (only if dev passed)
        6. Promote: replace production with validated pre_production

    If ANY step fails, production CSVs are untouched.

    Args:
        full_refresh: If True, re-scrape all data. Default False (incremental).
    """
    mode = "Full refresh" if full_refresh else "Incremental"
    checkpoint("Pipeline Starting", {
        "mode": mode,
        "production": str(PRODUCTION_DIR),
        "pre_production": str(PRE_PRODUCTION_DIR),
        "backups": str(BACKUPS_DIR),
    })

    # Step 1: Copy production into pre_production (working area)
    prepare_working_copy()

    # Step 2: Scrape latest data (writes into pre_production via env var)
    scrape_result = scrape(headless=True, full_refresh=full_refresh)

    # Step 3: Load pre_production CSVs to BigQuery
    load_result = load_to_bigquery()

    # Step 4: Build + test in dev (this is the gate)
    dbt_build_dev()

    # Step 5: Promote to prod (only reached if dev passed)
    dbt_build_prod()

    # Step 6: Everything passed — promote pre_production to production
    promote_to_production()

    # --- Final summary ---
    after = scrape_result["after"]
    checkpoint("PIPELINE COMPLETE", {
        "data": f"{after['standings']} standings, {after['matches']} matches, {after['match_events']} events",
        "bigquery": f"{sum(load_result.values())} total rows loaded to {BQ_DATASET}",
        "dev": "PASSED",
        "prod": "PASSED",
    })


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Hockey pipeline")
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Re-scrape all data instead of incremental",
    )
    args = parser.parse_args()

    hockey_pipeline(full_refresh=args.full_refresh)
