# Substack

# TDB Data Product: Substack

**Author:** Alex Heston\n**Last Updated:** April 2026\n**Scope:** All Substack publications under The Daily Beast


---

## Overview

This document describes the analytics surface that covers all Substack publications under TDB. It is intended for both technical consumers (analytics) and non-technical stakeholders (editorial, growth, and product teams)

The data product is organized into three domains:


1. **Subscriber Analytics** — Who is subscribed, at what tier, and are they active?
2. **Revenue** — How much did we recognize this month, and what should we expect next month?
3. **Post Grain Performance** — How are individual posts performing across engagement, traffic, and subscriber conversion?


---

## Publications

The following Substack publications are currently covered by this data product:

* **Howl**
* **The Royalist**
* **Punch Up**
* **Primal Scream**
* **The Swamp**


---

## Data Sources

| Source | Description | Ingestion |
|--------|-------------|-----------|
| Substack subscriber snapshot | Full snapshot of all subscribers per publication — includes subscription status, tier, acquisition channel, and activity rating | substack subscriber endpoint - https://theroyalist.substack.com/api/v1/subscriber-stats |
| Stripe charges & subscriptions | Payment events, subscription lifecycle, billing intervals, and FX-converted amounts | Fivetran  |
| Apple App Store payouts | Bulk payout receipts for iOS in-app subscriptions, received via Stripe | Fivetran  |
| Post grain snapshot | Full snapshot of post grain performance data | substack post endpoint<https://theroyalist.substack.com/api/v1/post_management/detail/> |

> **Note on the Substack export:** The export is a point-in-time snapshot. How frequently it is refreshed directly affects the freshness of subscriber counts and status flags.


---

## The Financial Calendar: 16th-to-15th Reporting Months

One of the most important structural decisions in this data product is how we define a "reporting month." Rather than using calendar months (January 1–31, etc.), TDB uses a **16th-to-15th convention**:

* "March 2026" means **March 16 – April 15**
* "April 2026" means **April 16 – May 15**

**Why?** This convention aligns with how subscription billing naturally lands in practice. Many Stripe charges occur mid-month, and slicing at the 16th gives finance a cleaner picture of revenue that "belongs" to a given reporting cycle — reducing the noise of charges that technically fall on the calendar boundary but are economically part of the prior period.

All revenue models — accrual, spreading, and projections — use this calendar consistently. When you filter by `reporting_month = 'April 2026'`, you are always getting March 16 – April 15.


---

## Domain 1: Subscriber Analytics

### What questions does this answer?

* How many paid subscribers does each publication have right now?
* Is a given subscriber on a monthly or annual plan?
* Are they active, cancelled-but-within-their-paid-window, or fully lapsed?
* How many subscribers came through iOS/Android vs. direct Stripe billing?
* What is our free vs. paid breakdown per publication?

### The core model: `fct__substack_subscriber_daily`

This is a **daily snapshot** table — one row per subscriber, per day. Every time the Substack export is refreshed, a new day's worth of rows is added. This lets you track how the subscriber base changes over time, not just what it looks like today.

The model combines two sources:

* **Substack** provides subscription identity: when someone subscribed, what tier they're on, whether they're a gift or comp, and Substack's own engagement score.
* **Stripe** provides billing context: the actual subscription status in Stripe, whether renewal is active, and whether the subscriber is set to cancel at period end.

The join between the two systems happens on **email address** (lowercased and trimmed for consistency).

#### The `payer_type` field

Identifies how a subscriber's access is funded. This is the primary field for segmenting paid subscribers by payment channel.

| `payer_type` | Meaning |
|-------------|---------|
| `stripe`    | Paid directly via Stripe (web or card billing) |
| `ios`       | Paid via Apple App Store in-app purchase (`paid_attribution = 'substack-ios-in-app-purchase'`) |
| `comp`      | Complimentary access — no payment |
| `gift`      | Gifted subscription |
| `free`      | Free subscriber with no payment |

`payer_type` is also present on `fct__stripe_substack_charge_accrual`, where `subscriber` rows carry `stripe` and `app_store` rows carry `ios`.

#### The `billing_interval` field

Resolved in priority order: (1) Stripe plans via `stg__stripe_plans` — most reliable; (2) Substack export `subscription_interval` — covers subscribers not matched to a Stripe plan; (3) `stripe_plan` name text parsing — last resort.

#### The `status_bucket` field

Subscription status is more nuanced than active/inactive. A subscriber who cancels mid-year is still entitled to their remaining paid period. `status_bucket` captures this:

| `status_bucket` | Meaning |
|---------------|---------|
| `Active`      | Subscription is current and paid |
| `Cancelled but Active` | Subscriber cancelled, but their paid period has not yet expired |
| `Expired`     | Subscription has lapsed — either cancelled past the end date, or the period ran out |
| `Non-paid`    | Free subscriber with no payment history |

#### The `is_active_paid` + `is_comp` flags

This is the single most useful boolean for "is this person a current paying customer?" It is `true` for both `Active` and `Cancelled but Active` because both of those subscribers have paid for access they are currently entitled to use. To exclude comp subscriptions (legacy, currently non-revenue generating) for a true "Who is paying right now" add the boolean flag `is_comp = false`.

### How to Query: Subscriber Analytics

**Current active paid subscribers by publication and payer type**

The most common query, a snapshot of today's paying subscriber base, broken down by publication and `payer_type`. Always filter to the most recent `snapshot_date` to avoid double-counting across historical snapshot rows.

```sql
SELECT
    publication,
    payer_type,
    billing_interval,
    COUNT(*) AS subscriber_count
FROM `data-platform-455517.substack.fct__substack_subscriber_daily`
WHERE
    snapshot_date = (SELECT MAX(snapshot_date) FROM `data-platform-455517.substack.fct__substack_subscriber_daily`)
    AND is_active_paid = TRUE
    AND is_comp = FALSE
GROUP BY 1, 2, 3
```

**Weekly active paid subscriber trend for a single publication**

Useful for tracking growth or churn momentum. Uses one snapshot per week (the Monday of each week) to reduce row volume.

```sql
SELECT
    DATE_TRUNC(snapshot_date, WEEK(MONDAY)) AS week_start,
    COUNT(*) AS active_paid_subscribers
FROM `data-platform-455517.substack.fct__substack_subscriber_daily`
WHERE
    publication = 'royalist'
    AND is_active_paid = TRUE
    AND is_comp = FALSE
    AND EXTRACT(DAYOFWEEK FROM snapshot_date) = 2  -- Monday only
GROUP BY 1
ORDER BY 1
```

**Free vs. paid breakdown on the latest snapshot**

```sql
SELECT
    publication,
    CASE
        WHEN is_active_paid THEN 'Paid'
        WHEN status_bucket = 'Non-paid' THEN 'Free'
        ELSE 'Lapsed / Other'
    END AS subscriber_class,
    COUNT(*) AS subscriber_count
FROM `data-platform-455517.substack.fct__substack_subscriber_daily`
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM `fct__substack_subscriber_daily`)
GROUP BY 1, 2
ORDER BY publication, subscriber_count DESC
```

**Look up a specific subscriber's current status**

```sql
SELECT
    snapshot_date,
    publication,
    email,
    payer_type,
    status_bucket,
    billing_interval,
    is_active_paid,
    is_non_stripe_paid,
    expiration_date
FROM `data-platform-455517.substack.fct__substack_subscriber_daily`
WHERE
    LOWER(email) = 'subscriber@example.com'
ORDER BY snapshot_date DESC
LIMIT 1
```


---

### Subscriber Snapshot Handoff to Analytics: `ai-mvp-392019.substack.fct__substack_royalist_compat`

Subscriber snapshot marts for each publication currently live in `ai-mvp-392019.substack.<publication>_subscribers`) that downstream consumers (dashboards, scripts, etc) are built against. In order to keep current analytics workflows intact we created a wrapper that maps currently referenced columns to our new underlying snapshot tables. This allows for existing SQL pipelines to remain while we just swap out the base table used in the first CTE. 

```sql
--CURRENT BASE CTE FOR SUBSCRIBER SNAPSHOT
WITH max_day AS (
  SELECT MAX(snapshot_date) AS last_day
  FROM `ai-mvp-392019.substack.royalist_daily_snapshot`
),
--HANDOFF USING NEW TRANSFORMATION LAYER
WITH max_day AS (
  SELECT MAX(date(snapshot_date)) AS last_day
  FROM `data-platform-455517.substack.fct__substack_royalist_compat`
),
```



---

## Domain 2: Revenue

### What questions does this answer?

* How much revenue did we recognize in a given reporting month?
* What portion came from monthly subscribers vs. annual subscribers vs. Apple App Store?
* How does month-over-month revenue trend?
* What does the finance team see in their monthly revenue report?
* What should we expect to recognize in the current in-progress month?
* Are we tracking ahead of or behind projection?

### Why "accrual" and not just "charges"?

A subscriber who pays $60 for an annual subscription in January has not "given us $60 of January revenue." Under accrual accounting, we recognize $5/month over 12 months. The accrual models handle this spreading automatically — a single $60 charge in January becomes 12 rows of $5, one per reporting month, through December.

Monthly subscribers are simpler: their $7 charge is recognized in full in the month it occurs.

### Revenue streams

| Stream | Model | Notes |
|--------|-------|-------|
| Monthly Stripe subscribers | `fct__stripe_substack_charge_accrual` | Recognized in full in month of charge, net of refunds |
| Annual Stripe subscribers | `fct__stripe_substack_charge_accrual` | Spread as 1/12 per reporting month across 12 months, net of refunds |
| Apple App Store payouts | `fct__stripe_substack_charge_accrual` | Bulk payout, not subscriber-linked; recognized in payout month |

### Refund handling

Refunds are deducted from revenue at the earliest possible layer — the `stg__stripe_charges` staging model — so every downstream model automatically reflects net-of-refund revenue without any additional logic.

#### How it works

**Step 1 — `stg__stripe_refunds`** pulls all succeeded Stripe refunds and joins each one to its balance transaction to get the actual net amount returned (converted from cents to dollars):

```sql
with source as (
    select
        id                      as refund_id,
        charge_id,
        balance_transaction_id,
        created                 as refunded_at,
        status
    from stripe.refund
    where status = 'succeeded'
),

balance_transactions as (
    select
        id,
        abs(net) / 100.0        as refunded_net_usd
    from stripe.balance_transaction
)

select
    r.refund_id,
    r.charge_id,
    r.refunded_at,
    bt.refunded_net_usd
from source r
left join balance_transactions bt
    on bt.id = r.balance_transaction_id
```

**Step 2 — `stg__stripe_charges`** aggregates total refunded amounts per charge and subtracts them from the charge's `net_amount_usd`:

```sql
refunds as (
    select
        charge_id,
        sum(refunded_net_usd)   as total_refunded_net_usd
    from stg__stripe_refunds
    group by 1
)

select
    ...
    bt.net_amount_usd - coalesce(r.total_refunded_net_usd, 0) as net_amount_usd,
    ...
from charges c
left join balance_transactions bt on bt.id = c.balance_transaction_id
left join refunds r               on r.charge_id = c.charge_id
```

The `coalesce(..., 0)` ensures charges with no refund are unaffected.

#### How refunds flow into the closed accrual model

`int__stripe_substack_charges` reads `net_amount_usd` from `stg__stripe_charges` (already refund-adjusted) and uses it to compute `recognized_revenue_usd`:

- **Monthly charges:** `recognized_revenue_usd = net_amount_usd` — a refund reduces the single recognized row directly.
- **Annual charges:** `recognized_revenue_usd = net_amount_usd / 12.0` per spread row — a refund reduces the base amount before it is divided across the 12 months, so all twelve recognition rows are proportionally lower.

`fct__stripe_substack_charge_accrual` and `fct__stripe_substack_month_accrual` simply aggregate these already-adjusted values upward, so the monthly rollup and the `stripe_recognized_revenue_usd` column in the finance report already reflect refunds with no further adjustment needed.

#### How refunds flow into the projection model

`fct__stripe_substack_month_revenue_projections` builds its EWMA trend from `fct__stripe_substack_month_accrual`, which is net of refunds. This means:

- **Closed rows** — `actual_revenue` and `recurring_revenue` already exclude refunded amounts.
- **Live row** — `revenue_to_date` is pulled from `fct__stripe_substack_charge_accrual`, also net of refunds.
- **Projected revenue** — the EWMA is computed over net-of-refund historical actuals, so the projection naturally reflects a revenue base that excludes returned payments.

A large or unusual refund in a given month will reduce that month's recognized revenue, which may modestly dampen the EWMA-projected trend for the following month (subject to the outlier cap described below).

### The charge-grain model: `fct__stripe_substack_charge_accrual`

This is the most granular revenue model — one row per monthly charge, twelve rows per annual charge (one per reporting month of the spread). If you need to trace a specific payment to a specific subscriber, this is where to look. All revenue figures are net of any refunds issued against that charge.

Annual charge rows include a `month_offset` field (0 through 11) so you can see where each row sits in the 12-month spread.

### The monthly rollup: `fct__stripe_substack_month_accrual`

This is the **primary model for finance reporting.** It rolls up the charge-grain model to three rows per reporting month:


1. Monthly subscriber revenue
2. Annual subscriber revenue (the 1/12 slice recognized this month)
3. Apple App Store revenue

If you want to know "what was our total recognized revenue in March 2026?", sum `recognized_revenue_usd` across all three rows where `reporting_month = 'March 2026'`.

### How to Query: Revenue

**Total recognized revenue for a given reporting month**

This is the canonical finance query. Three rows come back — one per revenue stream — and the sum is the month's total recognized revenue.

```sql
SELECT
  reporting_month,
  reporting_month_start,
  reporting_month_end,
  revenue_type,
  billing_interval,
  stripe_recognized_revenue_usd
FROM `ai-mvp-392019.substack.fct__stripe_substack_month_accrual`
WHERE reporting_month = 'March 2026'
ORDER BY revenue_type, billing_interval
```

**Month-over-month revenue trend by billing interval**

Shows how monthly and annual subscriber revenue has moved over time. Useful for identifying growth trends or seasonal patterns.

```sql
SELECT
    reporting_month,
    reporting_month_start,
    SUM(CASE WHEN billing_interval = 'monthly' THEN recognized_revenue_usd END) AS monthly_sub_revenue,
    SUM(CASE WHEN billing_interval = 'annual'  THEN recognized_revenue_usd END) AS annual_sub_revenue,
    SUM(CASE WHEN revenue_type = 'app_store'   THEN recognized_revenue_usd END) AS app_store_revenue,
    SUM(recognized_revenue_usd)                                                  AS total_revenue
FROM `ai-mvp-392019.substack.fct__stripe_substack_month_accrual`
GROUP BY 1, 2
ORDER BY reporting_month_start DESC
```

**Trace an annual charge across its 12-month spread**

If you want to see how a specific annual subscription charge is recognized month by month, query the charge-grain model using the `charge_id`. You'll get 12 rows, each showing the reporting month it's recognized in and the 1/12 monthly slice.

```sql
SELECT
    charge_id,
    email,
    charged_at,
    settled_amount_usd,
    billing_interval,
    month_offset,
    reporting_month,
    reporting_month_start,
    reporting_month_end,
    recognized_revenue_usd
FROM `ai-mvp-392019.substack.fct__stripe_substack_charge_accrual`
WHERE charge_id = 'ch_abc123'
ORDER BY month_offset
```


---

### Revenue Projections

#### The projection model: `fct__stripe_substack_month_revenue_projections`

This model produces two types of rows:

* **Closed rows** — one per past reporting month. These show actual revenue, actual starts and churn counts, and the projection that was live at the time. You can use `pct_of_projection_achieved` to see how accurate past projections were (positive = over-projected, negative = under-projected).
* **Live row** — one for the current in-progress month. This shows revenue recognized so far, prorated starts and churn, and the full-month projection.

**This view does not include app store transactions.**

#### How the projection works: EWMA

Rather than a simple average, we use an **Exponentially Weighted Moving Average (EWMA)** of revenue deltas, the month-over-month changes in recognized revenue, looking back five months. More recent months are weighted more heavily than older ones (weights: 16, 8, 4, 2, 1).

In plain language: we look at how revenue has been changing over the past five months, give more weight to what happened recently, and project that trend forward. If revenue has been growing by $300/month on average (with recent months trending higher), the model will project that growth to continue.

The model requires at least five months of history to produce a reliable projection. Earlier months may be less accurate.

#### The outlier cap

A single anomalous month, say, a promotional spike that brought in an unusual number of annual subscribers in November would distort the EWMA and cause the following month's projection to look artificially high. The model guards against this with an **outlier cap**: if any single month's delta is more than 1.5× the trimmed average (removing least and greatest values in set from avg calc) of the other four months, it is replaced by that trimmed average before being fed into the EWMA.

This means one unusual month cannot single-handedly throw off the projection. Because of this cap when investigating why a projection looks lower than expected after a strong month.

#### Reading the live row

When looking at the current in-progress month:

| Column | What it means |
|--------|---------------|
| `actual_revenue` | Revenue recognized so far in this period (charges that have already processed) |
| `projected_revenue` | Full-month estimate based on EWMA of past trends |
| `pct_of_projection_achieved` | What % of the projection you've recognized so far |
| `starts_prorated` | Estimated full-month new subscriber count based on pace to date |
| `churn_prorated` | Estimated full-month churn count based on pace to date |

#### Known limitations

The projection model is trend-based. It cannot anticipate:

* Promotional campaigns or editorial spikes that drive unusual subscriber volume
* Price changes
* New publication launches
* Seasonality patterns with fewer than 12 months of history

Accuracy improves with more historical data, and should be re-evaluated after any major business change.

### How to Query: Revenue Projections

**Check the current month's live projection**

Returns the in-progress reporting month — how much has been recognized so far, what the full-month projection is, and what percentage of the projection has been achieved to date.

```sql
SELECT
    reporting_month,
    target_start,
    target_end,
    billing_interval,
    pct_period_elapsed,
    actual_revenue,
    projected_revenue,
    ROUND(actual_revenue / projected_revenue * 100, 1) AS pct_of_projection_achieved,
    starts_prorated,
    churn_prorated
FROM `fct__stripe_substack_month_revenue_projections`
WHERE row_type = 'live'
ORDER BY billing_interval
```


---

## Domain 3: Post Grain Performance

### What questions does this answer?

* How did a specific post perform on email opens, click-throughs, and engagement?
* Where did readers come from — email, search, social, direct?
* Did a post drive net new subscribers or trigger unusual churn?
* How does a post compare to the publication's recent average?
* What are readers saying in the comments?

### Overview

Each Substack post generates data across four dimensions — overview/engagement, traffic sources, subscription conversions, and comments. These are modeled as four separate fact tables so you can query only what you need, and join on `post_id` when you need a fuller picture.

All four post grain tables share the same deduplication approach: Substack exports are point-in-time snapshots, so the same post may appear across multiple snapshot dates as stats update. The intermediate layer resolves this by keeping only the latest snapshot per post. One important nuance: certain fields (like `restacks`, `likes`, and `engaged`) can go null in newer snapshots even though the previous snapshot had data. The model uses `LAST_VALUE() IGNORE NULLS` window functions to preserve the last known value rather than overwriting good data with a null.


---

### `fct__substack_post_overview`

The primary post performance table. One row per post. Covers the full engagement funnel from email delivery through subscription conversion.

**Key field groups:**

| Group | What's included |
|-------|-----------------|
| Post metadata | `post_id`, `publication`, `title`, `post_type`, `audience`, `post_date`, `email_sent_at` |
| Email delivery | `queued`, `sent`, `delivered`, `dropped` |
| Engagement | `opened`, `open_rate`, `clicked`, `click_through_rate`, `engagement_rate`, `views`, `engaged`, `subscribers_finished_post`, `restacks`, `likes` |
| Conversions | `subscribes`, `monthly_subscribes`, `annual_subscribes`, `free_to_paid_upgrades`, `signups` |
| Churn signals | `unsubscribes`, `unsubscribes_within_1_day`, `disables_within_1_day` |
| Publication benchmarks | `pub_avg_*` fields — rolling averages across recent posts for the same publication, used to contextualize any individual post's performance |

The `pub_avg_*` benchmark fields are particularly useful: they let you answer "was this open rate above or below what we normally see?" without needing a separate calculation.


---

### `fct__substack_post_traffic`

Traffic sources for each post, pivoted wide. One row per post. Useful for understanding how readers arrived — whether through the email newsletter, organic search, social shares, or direct navigation.

Traffic is broken out across three lenses:

**By referrer category** (`referrer_views_*`): Email, Direct, Substack, Search, Social, News, Other External, Other Internal, Other

**By paid vs. free reader** (`paid_views_*`, `free_views_*`): Same categories above, split by whether the reader is a paid subscriber

**By device type** (`device_views_*`): Email client, Desktop Web, Mobile Web, Substack App


---

### `fct__substack_post_growth`

Subscription conversions and churn directly attributed to each post. One row per post.

| Column | Meaning |
|--------|---------|
| `subscribes` | Total new subscriptions from this post |
| `monthly_subscribes` | New monthly paid subscriptions |
| `annual_subscribes` | New annual paid subscriptions |
| `free_trials` | New free trial starts |
| `founding_subscribes` | Founding member conversions |
| `signups` | New free signups (email list) |
| `unsubscribes` | Unsubscribes attributed to this post |

This table answers "did this post grow or shrink the list?" at a post level. Joining to `fct__substack_post_overview` on `post_id` gives you the full picture of engagement alongside the growth outcome.


---

### `fct__substack_post_comments`

One row per comment (including replies). The body field contains plain text and is suitable for LLM-based analysis — sentiment scoring, topic clustering, or surfacing notable reader feedback.

| Column | Meaning |
|--------|---------|
| `comment_id` | Unique comment identifier |
| `post_id` | Parent post |
| `parent_comment_id` | Null for top-level comments; populated for replies |
| `body` | Plain text comment content |
| `snapshot_date` | When this comment was captured |


---

### How to Query: Post Grain Performance

**Top performing posts by open rate for a publication**

Compares each post's open rate to the publication's rolling average. A quick way to identify standout content.

```sql
SELECT
    post_id,
    publication,
    title,
    post_date,
    sent,
    open_rate,
    pub_avg_open_rate,
    ROUND((open_rate - pub_avg_open_rate) / NULLIF(pub_avg_open_rate, 0) * 100, 1) AS pct_above_avg,
    click_through_rate,
    subscribes
FROM `data-platform-455517.substack.fct__substack_post_overview`
WHERE
    publication = 'royalist'
    AND date(post_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND sent > 0
ORDER BY open_rate DESC
LIMIT 20
```

**Posts that drove the most paid conversions**

Useful for editorial teams trying to understand what content converts free readers to paying subscribers.

```sql
SELECT
    o.post_id,
    o.publication,
    o.title,
    o.post_date,
    o.views,
    o.open_rate,
    g.subscribes,
    g.monthly_subscribes,
    g.annual_subscribes,
    g.unsubscribes,
    (g.subscribes - g.unsubscribes) AS net_subscriber_change
FROM `data-platform-455517.substack.fct__substack_post_overview` o
JOIN `data-platform-455517.substack.fct__substack_post_growth` g USING (post_id)
WHERE date(o.post_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
ORDER BY g.subscribes DESC
LIMIT 25
```

**Traffic source breakdown for a specific post**

Shows where readers came from and whether they were paid or free.

```sql
SELECT
    post_id,
    referrer_views_email,
    referrer_views_search,
    referrer_views_social,
    referrer_views_direct,
    referrer_views_substack,
    paid_views_email,
    paid_views_search,
    paid_views_social,
    free_views_email,
    free_views_search,
    free_views_social,
    device_views_email,
    device_views_desktop_web,
    device_views_mobile_web,
    device_views_substack_app
FROM `data-platform-455517.substack.fct__substack_post_traffic`
WHERE post_id = '12345678'
```

**All comments on a post, threaded**

Pulls top-level comments and replies together, ordered so replies appear beneath their parent.

```sql
SELECT
    c.comment_id,
    c.parent_comment_id,
    CASE WHEN c.parent_comment_id IS NULL THEN 'top-level' ELSE 'reply' END AS comment_type,
    c.body,
    c.snapshot_date
FROM `data-platform-455517.substack.fct__substack_post_comments` c
WHERE c.post_id = '12345678'
ORDER BY
    COALESCE(c.parent_comment_id, c.comment_id),  -- groups replies under parent
    c.parent_comment_id IS NOT NULL               -- top-level before replies
```

**Cross-publication post performance summary (last 30 days)**

High-level editorial dashboard view across all publications.

```sql
SELECT
    o.publication,
    COUNT(DISTINCT o.post_id)              AS posts_published,
    ROUND(AVG(o.open_rate) * 100, 1)       AS avg_open_rate_pct,
    ROUND(AVG(o.click_through_rate) * 100, 1) AS avg_ctr_pct,
    SUM(o.views)                           AS total_views,
    SUM(g.subscribes)                      AS total_new_subs,
    SUM(g.unsubscribes)                    AS total_unsubs,
    SUM(g.subscribes - g.unsubscribes)     AS net_sub_change
FROM `data-platform-455517.substack.fct__substack_post_overview` o
LEFT JOIN `data-platform-455517.substack.fct__substack_post_growth` g USING (post_id)
WHERE date(o.post_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY total_views DESC
```


---

## Model Lineage

The **staging layer** (`stg__*`) cleans and normalizes raw source data — standardizing column names, converting currency from cents to dollars, filtering failed charges, and flattening nested JSON from the Substack dashboard export.

```
Stripe (Fivetran)               Subscriber Snapshot           Substack dashboard endpoints (JSON)
        │                                │                               │
stg__stripe_charges          stg__substack_subscribers           stg__substack_post_overview
stg__stripe_refunds ──┘                  │                       stg__substack_post_traffic
stg__stripe_invoices         int__substack_subscriber_daily      stg__substack_post_growth
stg__stripe_subscriptions                │                       stg__substack_post_comments
stg__stripe_customers        fct__substack_subscriber_daily              │
        │                                │                       int__substack_post_overview
int__stripe_substack_charges fct__substack_royalist_compat       int__substack_post_traffic
        │                                                        int__substack_post_growth
int__stripe_appstore_payouts                                     int__substack_post_comments
        │                                                                │
fct__stripe_substack_charge_accrual                              fct__substack_post_overview
        │                                                        fct__substack_post_traffic
fct__stripe_substack_month_accrual                               fct__substack_post_growth
        │                                                        fct__substack_post_comments
fct__stripe_substack_month_revenue_projections
```

`stg__stripe_refunds` feeds into `stg__stripe_charges` (indicated by `──┘`) so the refund deduction happens at the staging layer and propagates through every downstream model automatically.

The **intermediate layer** (`int__*`) applies business logic — the Stripe-Substack email join, billing interval resolution, the 16th-to-15th calendar logic, annual charge spreading, and deduplication of post snapshots to the latest known values.

The **mart layer** (`fct__*`) is what analysts and dashboards query. These are the models documented above.


---

## Maintenance Notes

### Known data quality considerations

**Non-Stripe paid subscribers (**`**is_non_stripe_paid**`**):** These subscribers have a `first_payment_at` timestamp from Substack but no matching Stripe subscription. They are Apple or Google in-app purchasers. Their revenue appears at the publication level in the App Store payout stream, not at the individual subscriber level.

**Email matching:** The Stripe-Substack join is done on email. If a subscriber uses different email addresses across the two systems, they will appear as unmatched in the subscriber daily model (`stripe_subscription_id` will be null). This affects a small number of subscribers and does not affect the revenue accrual models, which are Stripe-native.

**EWMA requires 5 months of history:** The projection model produces less reliable output in the early months of a new publication or after a major revenue event resets the baseline.

**Refunds reduce the charge's net amount, not a separate line:** Refunds are applied by subtracting from `net_amount_usd` on the original charge row, not as a standalone negative entry. This means you won't see refunds as separate rows in the accrual models — instead, the charge itself will show a lower (or zero) recognized revenue. If a charge was fully refunded, its `recognized_revenue_usd` will be 0 across all spread rows.