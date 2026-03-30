# tdb-transformation

dbt transformation pipeline for The Daily Beast's Substack analytics. Pulls pre-aggregated post performance data from the Substack dashboard API and produces clean, analytics-ready tables in BigQuery.

> **Note:** The file structure and Substack publication below reflect one example (The Royalist). Additional publications and data sources will follow the same pattern.

## Project structure

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

## Data sources

Raw JSON payloads are landed by an acquisition script into BigQuery (`data-platform-455517.raw_landing`) with one row per post per snapshot date.

| Source table | Description |
|---|---|
| `substack_royalist___post_overview` | Overview tab payload — post metadata, email delivery stats, engagement, subscription conversions, churn signals, publication benchmarks |

## Models

### `fct__substack_posts`
One row per post. Includes:
- Post metadata (title, slug, author, audience, publish date)
- Email delivery stats (sent, delivered, opened, clicked, open rate, CTR)
- Engagement (views, likes, comments, restacks, engagement rate)
- Subscription conversions (signups, subscribes, estimated value)
- Churn signals (unsubscribes, disables within 1 day)
- Publication benchmarks (`pub_avg_*`) — rolling averages across recent comparable posts for direct benchmarking

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
