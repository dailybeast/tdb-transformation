# tdb-transformation

dbt transformation pipeline for The Daily Beast. Produces clean, analytics-ready tables in BigQuery from raw data landed by acquisition scripts.

## Current use cases

### Substack subscriber analytics

Daily snapshot of all subscribers across TDB's Substack publications. Tracks subscription tier, status, and activity over time.

```
models/
├── staging/
│   └── substack/
│       └── stg__substack_subscribers          # Normalizes Substack export snapshot
├── intermediate/
│   └── substack/
│       ├── int__substack_subscribers          # Email-joins Substack + Stripe subscription data
│       └── int__substack_subscriber_daily     # One row per subscriber per snapshot date
└── mart/
    └── substack/
        ├── fct__substack_subscriber_daily     # Final daily snapshot fact table (incremental)
        └── fct__substack_royalist_compat      # Backward-compat wrapper for legacy analytics pipelines
```

`fct__substack_subscriber_daily` — one row per subscriber per snapshot date. Includes `type_bucket` (billing tier), `status_bucket` (active/cancelled/expired/non-paid), and `is_active_paid` boolean.

### Substack post-grain performance

Post-level engagement, traffic, growth, and comments across all TDB Substack publications.

```
models/
├── staging/
│   └── substack/
│       ├── stg__substack_post_overview        # Flattens raw JSON, enforces contract
│       ├── stg__substack_post_traffic         # Traffic source breakdown
│       ├── stg__substack_post_growth          # Subscription conversion/churn per post
│       └── stg__substack_post_comments        # Comment body and threading data
├── intermediate/
│   └── substack/
│       ├── int__substack_post_overview        # Dedupes to latest snapshot, fills LAST_VALUE nulls
│       ├── int__substack_post_traffic
│       ├── int__substack_post_growth
│       └── int__substack_post_comments
└── mart/
    └── substack/
        ├── fct__substack_post_overview        # One row per post; engagement, delivery, conversions, benchmarks
        ├── fct__substack_post_traffic         # Traffic sources pivoted wide, split paid/free/device
        ├── fct__substack_post_growth          # Conversions and churn attributed per post
        └── fct__substack_post_comments        # One row per comment/reply
```

### Stripe subscription revenue

Revenue accounting for Substack subscriber payments processed through Stripe.

```
models/
├── staging/
│   └── stripe/
│       ├── stg__stripe_charges              # Succeeded charges, refund-adjusted at this layer
│       ├── stg__stripe_refunds              # Succeeded refunds joined to balance transactions
│       ├── stg__stripe_invoices             # Invoice records with subscription linkage
│       ├── stg__stripe_plans                # Plan definitions (advertised price, interval)
│       ├── stg__stripe_subscriptions        # Subscription lifecycle and status
│       ├── stg__stripe_subscription_items   # Links subscriptions to their plans
│       └── stg__stripe_customers            # Customer records
├── intermediate/
│   └── stripe/
│       ├── int__stripe_substack_subscriptions  # Subscription-grain enriched with plan metadata
│       ├── int__stripe_substack_charges        # Charge-grain: billing interval resolved, 16th-to-15th month assigned
│       └── int__stripe_appstore_payouts        # Apple App Store bulk payouts isolated from charge stream
└── mart/
    └── stripe/
        ├── fct__stripe_substack_charge_accrual          # One row per monthly charge; 12 rows per annual charge (spread)
        ├── fct__stripe_substack_month_accrual           # Revenue rolled up to reporting month
        └── fct__stripe_substack_month_revenue_projections  # Forward-looking EWMA revenue projections
```

#### The financial calendar: 16th-to-15th reporting months

Reporting months use a 16th-to-15th convention rather than calendar months. A charge on or after the 16th belongs to the following named month; a charge on the 1st–15th belongs to the current named month. For example, a charge on March 20 is recognized in "April 2026" (April 16 – May 15).

This is applied in `int__stripe_substack_charges` and `int__stripe_appstore_payouts`.

#### Billing interval resolution

`int__stripe_substack_charges` resolves `billing_interval` using three signals in priority order:

1. Substack export's `subscription_interval` field (`annual` or `monthly`) — most reliable
2. Fallback: `settled_amount_usd >= 50` → `annual`, otherwise `monthly` — covers subscribers not matched in the Substack export

#### Refund handling

Refunds are deducted from `net_amount_usd` inside `stg__stripe_charges` (via a left join to `stg__stripe_refunds`), so every downstream model automatically reflects net-of-refund amounts. Refunds do not appear as separate negative rows in any accrual model — the originating charge row simply carries a lower (or zero) `net_amount_usd`.

#### Key amount fields

| Field | Definition |
|---|---|
| `settled_amount_usd` | Gross amount collected from the subscriber, including any applicable sales tax. |
| `net_amount_usd` | Amount after Stripe processing fee and Substack platform fee, net of any refunds. This is what hits The Daily Beast's account. |
| `recognized_revenue_usd` | For **monthly** charges: equal to `net_amount_usd`. For **annual** charges: `net_amount_usd / 12.0` — recognized over 12 reporting months via a cross join on `generate_array(0, 11)`. App Store payouts: equal to `net_amount_usd`, recognized in full in the payout month. |

#### App Store payout identification

Apple App Store payouts arrive as Stripe charges whose `description` matches `'Earnings from App Store subscriptions for%'`. They are isolated in `int__stripe_appstore_payouts`, carry no subscriber-level linkage, and are recognized in full in the reporting month of the payout.

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
