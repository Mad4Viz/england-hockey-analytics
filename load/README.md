# Load Module

This module handles loading scraped CSV data into Google BigQuery.

---

## Current Approach: Manual CLI

Data is loaded using the `bq` command-line tool with `--autodetect` for schema inference:

```bash
# Navigate to project root
cd /path/to/england-hockey-analytics

# Load standings data
bq load --source_format=CSV --autodetect --replace \
  england-hockey-analytics:hockey_raw.standings \
  data/production/standings.csv

# Load matches data
bq load --source_format=CSV --autodetect --replace \
  england-hockey-analytics:hockey_raw.matches \
  data/production/matches.csv

# Load match events data
bq load --source_format=CSV --autodetect --replace \
  england-hockey-analytics:hockey_raw.match_events \
  data/production/match_events.csv
```

### Why Manual?

- **Simplicity**: For a portfolio project with periodic refreshes, CLI commands are sufficient
- **Transparency**: Commands are explicit and easy to understand
- **No dependencies**: Uses only the `gcloud` SDK (no additional Python packages)

---

## Data Locations

| Dataset | Location | Purpose |
|---------|----------|---------|
| `data/sample/` | GitHub (tracked) | Sample data for repo visitors |
| `data/production/` | Local only (gitignored) | Full dataset for analysis |

---

## Future Enhancements

For a production pipeline, consider:

### 1. Python Automation

```python
from google.cloud import bigquery

client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

with open("data/production/standings.csv", "rb") as f:
    job = client.load_table_from_file(
        f, "hockey_raw.standings", job_config=job_config
    )
    job.result()  # Wait for completion
```

### 2. GCS Intermediate Layer

```
Scraper → GCS Bucket → BigQuery (batch load) → dbt
```

Benefits:
- **Free loading**: BigQuery batch loads from GCS are free (vs streaming inserts)
- **Audit trail**: Raw files preserved in cloud storage
- **Decoupling**: Extract and load stages are independent
- **Scalability**: Handles larger datasets efficiently

### 3. Orchestration

Tools like Airflow, Prefect, or Cloud Composer could automate the full pipeline:
1. Trigger scraper on schedule
2. Upload CSVs to GCS
3. Load from GCS to BigQuery
4. Run dbt build
5. Refresh Tableau data source

---

## BigQuery Datasets

| Dataset | Purpose |
|---------|---------|
| `hockey_raw` | Raw scraped data (CSV uploads land here) |
| `hockey_dev` | dbt development environment |
| `hockey_prod` | dbt production environment (Tableau connects here) |
