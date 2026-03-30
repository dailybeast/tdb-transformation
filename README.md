# tdb-transformation

dbt transformation pipeline for The Daily Beast. Produces clean, analytics-ready tables in BigQuery from raw data landed by acquisition scripts.

## Setup

```bash
cd tdb_transformation
dbt deps
dbt run
dbt test
```

## Requirements

- Python 3.9+
- dbt-bigquery 1.10+
- BigQuery credentials with Data Editor access on the target dataset
