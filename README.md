# tdb-transformation

dbt transformation pipeline for The Daily Beast. Produces clean, analytics-ready tables in BigQuery from raw data landed by acquisition scripts.

## Current use cases

### Substack

Post-performance analytics for The Daily Beast's Substack publications. Pulls pre-aggregated data from the Substack dashboard API.

```
models/
├── staging/
│   └── substack/
│       └── stg__substack_post_overview    # Flattens raw JSON payload, enforces contract
├── intermediate/
│   └── int__substack_post_overview        # Dedupes to latest snapshot, casts timestamps
└── mart/
    └── substack/
        └── fct__substack_posts            # Final post-grain fact table
```

`fct__substack_posts` — one row per post, includes email delivery stats, engagement, subscription conversions, churn signals, and publication benchmarks (`pub_avg_*`).

## Setup

```bash
cd tdb_transformation
dbt deps
dbt run
dbt test
```

Check source freshness:
```bash
dbt source freshness
```

## Requirements

- Python 3.9+
- dbt-bigquery 1.10+
- BigQuery credentials with Data Editor access on the target dataset
