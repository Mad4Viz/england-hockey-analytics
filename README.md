# England Hockey Analytics Pipeline

End-to-end analytics engineering project: Python/Selenium scraping to BigQuery, dbt Core transformations, and Tableau MCP for natural language Q&A.

---

**Status:** In Development

Full documentation coming soon.

---

## Architecture

```
Scraper (Python/Selenium) → Local CSV → BigQuery → dbt → Tableau
```

| Stage | Folder | Description |
|-------|--------|-------------|
| Extract | `extract/` | Selenium scraper for England Hockey website |
| Load | `load/` | BigQuery upload (see [load/README.md](load/README.md)) |
| Transform | `transform/hockey_dbt/` | dbt Core models (staging → marts) |

### Sandbox Limitations

This project uses **BigQuery sandbox mode** (free tier, no billing required). Limitations:
- Tables expire after 60 days
- No Cloud Storage (GCS) integration

**Production architecture** would add GCS as an intermediate layer:
```
Scraper → GCS Bucket → BigQuery (free batch load) → dbt → BI Tool
```

The GCS step provides:
- Free data loading to BigQuery (vs paid streaming inserts)
- Raw file backup/audit trail
- Decoupled extract and load stages

---

## Data Quality

Known data quality notes:

| Issue | Count | Cause |
|-------|-------|-------|
| Missing venue | 212 | Matches without venue recorded in source |
| TBC teams | 8 | Placeholder teams for undetermined playoff matches |

These are source data limitations, not pipeline bugs. The dbt pipeline handles them gracefully with warnings.

**Note:** Phase data (Phase 1/2) only applies to Premier League competitions. Other competitions don't use this structure, so null phase values are expected.

---

## For Contributors

See [PROJECT_PLAN.md](PROJECT_PLAN.md) for the development roadmap and checklist.
