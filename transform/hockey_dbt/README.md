# Transform (dbt Core)

Transforms raw scraped data into a star schema using dbt Core on BigQuery.

---

## Model Layers

| Layer | Models | What it does |
|-------|--------|-------------|
| Staging | 3 | Cleans and standardises raw tables |
| Intermediate | 2 | Joins and reshapes for analysis |
| Marts | 8 | 4 dimension + 4 fact tables (star schema) |

13 models total, from 3 raw sources to 8 mart tables.

---

## Testing

87 schema tests run on every build:

| Test type | Count |
|-----------|-------|
| not_null | 49 |
| unique | 18 |
| relationships | 15 |
| accepted_values | 5 |

---

## CI/CD

Two GitHub Actions workflows in [`.github/workflows/`](../../.github/workflows/):

| Workflow | Trigger | Steps |
|----------|---------|-------|
| CI | Pull request to main | SQLFluff lint → dbt compile → dbt test |
| Deploy | Push to main | SQLFluff lint → dbt build (prod) → dbt test (prod) → dbt docs generate |

The mart tables in BigQuery prod connect to Tableau Cloud via the GCP connector. A data model is built in Tableau and published as a data source, which can then be queried through Claude Code via Tableau MCP. The [semantic layer](../../docs/semantic_layer.yml) defines the tables, relationships, and query guidelines.
