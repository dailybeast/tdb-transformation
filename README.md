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

### Stripe subscription revenue

Revenue accounting for Substack subscriber payments processed through Stripe.

```
models/
├── staging/
│   └── stripe/
│       ├── stg__stripe_charges          # Raw charge records, normalized
│       ├── stg__stripe_invoices         # Invoice records with line-item detail
│       ├── stg__stripe_plans            # Plan definitions (advertised price, interval)
│       ├── stg__stripe_refunds          # Refund records
│       └── stg__stripe_subscription_items  # Links subscriptions to their plans
├── intermediate/
│   └── stripe/
│       ├── int__stripe_substack_charges        # Charge-grain with revenue recognition logic
│       └── int__stripe_substack_subscriptions  # Subscription-grain enriched with plan metadata
└── mart/
    └── stripe/
        ├── fct__stripe_substack_charge_accrual          # One row per charge, with accrual dates
        ├── fct__stripe_substack_month_accrual           # Revenue rolled up to reporting month
        └── fct__stripe_substack_month_revenue_projections  # Forward-looking revenue projections
```

#### Key amount fields

| Field | Definition |
|---|---|
| `plan.amount_usd` | Advertised subscription price (e.g. $7.00). Pre-tax, pre-fee. Will diverge from actual charge amounts for subscribers in states where Stripe collects sales tax. |
| `settled_amount_usd` | Gross amount collected from the subscriber. Includes any applicable sales tax. This is what the subscriber's card is charged. |
| `net_amount_usd` | Amount retained after Stripe's processing fee (2.9% + $0.30) and Substack's platform fee (10%) are deducted. This is what hits The Daily Beast's account. |
| `recognized_revenue_usd` | Equal to `net_amount_usd`. This is the figure used in all accrual and projection models. |

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
