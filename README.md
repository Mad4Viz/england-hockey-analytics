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

## For Contributors

See [PROJECT_PLAN.md](PROJECT_PLAN.md) for the development roadmap and checklist.
