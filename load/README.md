# Load (BigQuery)

Uploads scraped CSVs into Google BigQuery. Each run overwrites the existing tables with the full CSV, rather than appending new rows. The load step is handled by `load_to_bigquery()` in [`orchestrate/hockey_flow.py`](../orchestrate/hockey_flow.py).

---

## Why Overwrite?

The CSVs already contain all data (historical + new). At this volume (~1MB total) reloading everything takes seconds, and it resets the BigQuery sandbox 60-day table expiry with every run.

---

## BigQuery Datasets

| Dataset | Purpose |
|---------|---------|
| `hockey_raw` | Raw scraped data (CSV uploads land here) |
| `hockey_dev` | dbt development environment |
| `hockey_prod` | dbt production environment (Tableau connects here) |
