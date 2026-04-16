# TDB Data Product: Substack

**Author:** Alex Heston  
**Last Updated:** April 2026  
**Scope:** All Substack publications under The Daily Beast

---

## Overview

This document describes the analytics surface that covers all Substack publications under TDB. It is intended for both technical consumers (analysts querying dbt models directly) and non-technical stakeholders (editorial, finance, and product teams) who want to understand what the data says and how it is produced.

The data product is organized into three domains:

1. **Subscriber Analytics** — Who is subscribed, at what tier, and are they active?
2. **Revenue** — How much did we recognize this month, and what should we expect next month?
3. **Post Grain Performance** — How are individual posts performing across engagement, traffic, and subscriber conversion?

Each domain draws from the same underlying sources — Substack's subscriber export and Stripe's payment records — but answers different business questions.

---

## Publications

The following Substack publications are currently covered by this data product:

- **Howl**
- **The Royalist**
- **Punch Up**
- **Primal Scream**
- **The Swamp**

---

## Data Sources

| Source | Description | Ingestion |
|---|---|---|
| Substack subscriber export | Full snapshot of all subscribers per publication — includes subscription status, tier, acquisition channel, and activity rating | Manual CSV export loaded to BigQuery |
| Stripe charges & subscriptions | Payment events, subscription lifecycle, billing intervals, and FX-converted amounts | Fivetran (automated) |
| Apple App Store payouts | Bulk payout receipts for iOS in-app subscriptions, received via Stripe | Fivetran (automated) |

> **Note on the Substack export:** The export is a point-in-time snapshot. How frequently it is refreshed directly affects the freshness of subscriber counts and status flags. This is a known limitation and a candidate for automation.

---

## The Financial Calendar: 16th-to-15th Reporting Months

One of the most important structural decisions in this data product is how we define a "reporting month." Rather than using calendar months (January 1–31, etc.), TDB uses a **16th-to-15th convention**:

- "April 2026" means **March 16 – April 15**
- "May 2026" means **April 16 – May 15**

**Why?** This convention aligns with how subscription billing naturally lands in practice. Many Stripe charges occur mid-month, and slicing at the 16th gives finance a cleaner picture of revenue that "belongs" to a given reporting cycle — reducing the noise of charges that technically fall on the calendar boundary but are economically part of the prior period.

All revenue models — accrual, spreading, and projections — use this calendar consistently. When you filter by `reporting_month = 'April 2026'`, you are always getting March 16 – April 15.

---

## Domain 1: Subscriber Analytics

### What questions does this answer?

- How many paid subscribers does each publication have right now?
- Is a given subscriber on a monthly or annual plan?
- Are they active, cancelled-but-within-their-paid-window, or fully lapsed?
- How many subscribers came through iOS/Android vs. direct Stripe billing?
- What is our free vs. paid breakdown per publication?

### The core model: `fct__substack_subscriber_daily`

This is a **daily snapshot** table — one row per subscriber, per day. Every time the Substack export is refreshed, a new day's worth of rows is added. This lets you track how the subscriber base changes over time, not just what it looks like today.

The model combines two sources:

- **Substack** provides subscription identity: when someone subscribed, what tier they're on, whether they're a gift or comp, and Substack's own engagement score.
- **Stripe** provides billing context: the actual subscription status in Stripe, whether renewal is active, and whether the subscriber is set to cancel at period end.

The join between the two systems happens on **email address** (lowercased and trimmed for consistency).

#### The `type_bucket` field

Because Substack has several overlapping subscription concepts — gifted subscriptions, complimentary access, lifetime "Royal Tier" memberships, monthly and annual paid plans — we consolidate them into a single `type_bucket` field for ease of querying:

| `type_bucket` | Meaning |
|---|---|
| `Monthly Subscriber` | Paying on a monthly billing cycle |
| `Yearly Subscriber` | Paying on an annual billing cycle |
| `Royal Tier` | Lifetime membership |
| `Monthly Gift` | Gifted subscription, monthly |
| `Yearly Gift` | Gifted subscription, annual |
| `Comp` | Complimentary access (no payment) |

#### The `status_bucket` field

Subscription status is more nuanced than active/inactive. A subscriber who cancels mid-year is still entitled to their remaining paid period. `status_bucket` captures this:

| `status_bucket` | Meaning |
|---|---|
| `Active` | Subscription is current and paid |
| `Cancelled but Active` | Subscriber cancelled, but their paid period has not yet expired |
| `Expired` | Subscription has lapsed — either cancelled past the end date, or the period ran out |
| `Non-paid` | Free subscriber with no payment history |

#### The `is_active_paid` flag

This is the single most useful boolean for "is this person a current paying customer?" It is `true` for both `Active` and `Cancelled but Active` — because both of those subscribers have paid for access they are currently entitled to use.

#### A note on `billing_interval`

Stripe is the authoritative source for billing interval (monthly vs. annual). But some subscribers pay through iOS or Android — Apple and Google process those payments, not Stripe directly. For those subscribers (`is_non_stripe_paid = true`), we fall back to parsing the Substack plan name (e.g., `"$60 a year"` → annual, `"$7 a month"` → monthly). If even that fails, we use a $50 threshold: charges >= $50 are assumed to be annual. This is a known approximation.

### How to Query: Subscriber Analytics

**Current active paid subscribers by publication and tier**

The most common query — a snapshot of today's paying subscriber base, broken down by publication and `type_bucket`. Always filter to the most recent `snapshot_date` to avoid double-counting across historical snapshot rows.

```sql
SELECT
    publication,
    type_bucket,
    billing_interval,
    COUNT(*) AS subscriber_count
FROM `fct__substack_subscriber_daily`
WHERE
    snapshot_date = (SELECT MAX(snapshot_date) FROM `fct__substack_subscriber_daily`)
    AND is_active_paid = TRUE
GROUP BY 1, 2, 3
ORDER BY publication, subscriber_count DESC
```

**Weekly active paid subscriber trend for a single publication**

Useful for tracking growth or churn momentum. Uses one snapshot per week (the Monday of each week) to reduce row volume.

```sql
SELECT
    DATE_TRUNC(snapshot_date, WEEK(MONDAY)) AS week_start,
    COUNT(*) AS active_paid_subscribers
FROM `fct__substack_subscriber_daily`
WHERE
    publication = 'royalist'
    AND is_active_paid = TRUE
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
FROM `fct__substack_subscriber_daily`
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
    type_bucket,
    status_bucket,
    billing_interval,
    is_active_paid,
    is_non_stripe_paid,
    subscription_expires_at
FROM `fct__substack_subscriber_daily`
WHERE
    LOWER(email) = 'subscriber@example.com'
ORDER BY snapshot_date DESC
LIMIT 1
```

---

### Legacy view: `fct__substack_royalist_compat`

The Royalist publication previously had its own standalone subscriber table (`royalist_daily_snapshot`) that downstream consumers — dashboards, scripts, exports — were built against. When we migrated to the unified `fct__substack_subscriber_daily` model, we created `fct__substack_royalist_compat` as a compatibility shim: a view that maps the new columns back to the old column names and semantics.

This view exists so that existing downstream consumers continue to work without modification. If you are building something new, use `fct__substack_subscriber_daily` directly.

---

## Domain 2: Revenue

### What questions does this answer?

- How much revenue did we recognize in a given reporting month?
- What portion came from monthly subscribers vs. annual subscribers vs. Apple App Store?
- How does month-over-month revenue trend?
- What does the finance team see in their monthly revenue report?
- What should we expect to recognize in the current in-progress month?
- Are we tracking ahead of or behind projection?

### Why "accrual" and not just "charges"?

A subscriber who pays $60 for an annual subscription in January has not "given us $60 of January revenue." Under accrual accounting, we recognize $5/month over 12 months. The accrual models handle this spreading automatically — a single $60 charge in January becomes 12 rows of $5, one per reporting month, through December.

Monthly subscribers are simpler: their $7 charge is recognized in full in the month it occurs.

### Revenue streams

| Stream | Model | Notes |
|---|---|---|
| Monthly Stripe subscribers | `fct__stripe_substack_charge_accrual` | Recognized in full in month of charge |
| Annual Stripe subscribers | `fct__stripe_substack_charge_accrual` | Spread as 1/12 per reporting month across 12 months |
| Apple App Store payouts | `fct__stripe_substack_charge_accrual` | Bulk payout, not subscriber-linked; recognized in payout month |

### The charge-grain model: `fct__stripe_substack_charge_accrual`

This is the most granular revenue model — one row per monthly charge, twelve rows per annual charge (one per reporting month of the spread). If you need to trace a specific payment to a specific subscriber, this is where to look.

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
    recognized_revenue_usd,
    subscriber_count
FROM `fct__stripe_substack_month_accrual`
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
FROM `fct__stripe_substack_month_accrual`
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
FROM `fct__stripe_substack_charge_accrual`
WHERE charge_id = 'ch_abc123'
ORDER BY month_offset
```

---

### Revenue Projections

#### Why build a projection model at all?

The reporting month doesn't close until the 15th, but finance needs forward visibility before that. The projection model gives a best estimate of where the month will land, updated daily as new charges come in. It also provides a track record of past projections vs. actuals, so you can evaluate model accuracy over time.

#### The projection model: `fct__stripe_substack_month_revenue_projections`

This model produces two types of rows:

- **Closed rows** — one per past reporting month. These show actual revenue, actual starts and churn counts, and the projection that was live at the time. You can use `pct_of_projection_achieved` to see how accurate past projections were (positive = over-projected, negative = under-projected).
- **Live row** — one for the current in-progress month. This shows revenue recognized so far, prorated starts and churn, and the full-month projection.

#### How the projection works: EWMA

Rather than a simple average, we use an **Exponentially Weighted Moving Average (EWMA)** of revenue deltas — the month-over-month changes in recognized revenue — looking back five months. More recent months are weighted more heavily than older ones (weights: 16, 8, 4, 2, 1).

In plain language: we look at how revenue has been changing over the past five months, give more weight to what happened recently, and project that trend forward. If revenue has been growing by $300/month on average (with recent months trending higher), the model will project that growth to continue.

The model requires at least five months of history to produce a reliable projection. Earlier months may be less accurate.

#### The outlier cap

A single anomalous month — say, a promotional spike that brought in an unusual number of annual subscribers in November — would distort the EWMA and cause the following month's projection to look artificially high. The model guards against this with an **outlier cap**: if any single month's delta is more than 1.5× the trimmed average of the other four months, it is replaced by that trimmed average before being fed into the EWMA.

This means one unusual month cannot single-handedly throw off the projection. The cap fires automatically — you don't need to do anything manually — but it is worth being aware of when investigating why a projection looks lower than expected after a strong month.

#### Reading the live row

When looking at the current in-progress month:

| Column | What it means |
|---|---|
| `actual_revenue` | Revenue recognized so far in this period (charges that have already processed) |
| `projected_revenue` | Full-month estimate based on EWMA of past trends |
| `pct_of_projection_achieved` | What % of the projection you've recognized so far |
| `starts_prorated` | Estimated full-month new subscriber count based on pace to date |
| `churn_prorated` | Estimated full-month churn count based on pace to date |

#### Known limitations

The projection model is trend-based. It cannot anticipate:

- Promotional campaigns or editorial spikes that drive unusual subscriber volume
- Price changes
- New publication launches
- Seasonality patterns with fewer than 12 months of history

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

**Historical projection accuracy — how well has the model done?**

Closed rows let you audit the model's track record. `pct_of_projection_achieved` on closed rows shows the final miss: positive means the projection was too high, negative means it was too low.

```sql
SELECT
    reporting_month,
    billing_interval,
    actual_revenue,
    projected_revenue,
    pct_of_projection_achieved AS final_pct_miss,
    starts,
    churn
FROM `fct__stripe_substack_month_revenue_projections`
WHERE row_type = 'closed'
ORDER BY target_start DESC
LIMIT 12
```

**Side-by-side view: live month + last 3 closed months**

A quick dashboard-style query to show the current month in context.

```sql
SELECT
    reporting_month,
    row_type,
    billing_interval,
    actual_revenue,
    projected_revenue,
    pct_of_projection_achieved,
    starts,
    COALESCE(starts_prorated, CAST(starts AS FLOAT64)) AS starts_full_month_est,
    churn,
    COALESCE(churn_prorated, CAST(churn AS FLOAT64)) AS churn_full_month_est
FROM `fct__stripe_substack_month_revenue_projections`
WHERE
    row_type = 'live'
    OR target_start >= DATE_SUB(
        (SELECT MAX(target_end) FROM `fct__stripe_substack_month_revenue_projections` WHERE row_type = 'closed'),
        INTERVAL 3 MONTH
    )
ORDER BY target_start DESC, billing_interval
```

---

## Domain 3: Post Grain Performance

### What questions does this answer?

- How did a specific post perform on email opens, click-throughs, and engagement?
- Where did readers come from — email, search, social, direct?
- Did a post drive net new subscribers or trigger unusual churn?
- How does a post compare to the publication's recent average?
- What are readers saying in the comments?

### Overview

Each Substack post generates data across four dimensions — overview/engagement, traffic sources, subscription conversions, and comments. These are modeled as four separate fact tables so you can query only what you need, and join on `post_id` when you need a fuller picture.

All four post grain tables share the same deduplication approach: Substack exports are point-in-time snapshots, so the same post may appear across multiple snapshot dates as stats update. The intermediate layer resolves this by keeping only the latest snapshot per post. One important nuance: certain fields (like `restacks`, `likes`, and `engaged`) can go null in newer snapshots even though the previous snapshot had data. The model uses `LAST_VALUE() IGNORE NULLS` window functions to preserve the last known value rather than overwriting good data with a null.

---

### `fct__substack_post_overview`

The primary post performance table. One row per post. Covers the full engagement funnel from email delivery through subscription conversion.

**Key field groups:**

| Group | What's included |
|---|---|
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
|---|---|
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
|---|---|
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
FROM `fct__substack_post_overview`
WHERE
    publication = 'royalist'
    AND post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
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
FROM `fct__substack_post_overview` o
JOIN `fct__substack_post_growth` g USING (post_id)
WHERE o.post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
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
FROM `fct__substack_post_traffic`
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
FROM `fct__substack_post_comments` c
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
FROM `fct__substack_post_overview` o
LEFT JOIN `fct__substack_post_growth` g USING (post_id)
WHERE o.post_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1
ORDER BY total_views DESC
```

---

## Model Lineage

```
Stripe (Fivetran)               Substack export (CSV)          Substack dashboard (JSON)
        │                                │                               │
stg__stripe_charges          stg__substack_subscribers     stg__substack_post_overview
stg__stripe_invoices                     │                 stg__substack_post_traffic
stg__stripe_subscriptions    int__substack_subscriber_daily stg__substack_post_growth
stg__stripe_customers                    │                 stg__substack_post_comments
        │                    fct__substack_subscriber_daily          │
int__stripe_substack_charges             │                 int__substack_post_overview
        │                    fct__substack_royalist_compat  int__substack_post_traffic
int__stripe_appstore_payouts                               int__substack_post_growth
        │                                                  int__substack_post_comments
fct__stripe_substack_charge_accrual                                  │
        │                                                  fct__substack_post_overview
fct__stripe_substack_month_accrual                         fct__substack_post_traffic
        │                                                  fct__substack_post_growth
fct__stripe_substack_month_revenue_projections             fct__substack_post_comments
```

The **staging layer** (`stg__*`) cleans and normalizes raw source data — standardizing column names, converting currency from cents to dollars, filtering failed charges, and flattening nested JSON from the Substack dashboard export.

The **intermediate layer** (`int__*`) applies business logic — the Stripe-Substack email join, billing interval resolution, the 16th-to-15th calendar logic, annual charge spreading, and deduplication of post snapshots to the latest known values.

The **mart layer** (`fct__*`) is what analysts and dashboards query. These are the models documented above.

---

## Maintenance Notes

### Adding a new reporting month

Revenue accrual and projections are fully automated. When the 15th passes and a new reporting month opens, new charges will be picked up by the next dbt run and assigned to the correct reporting month automatically. No manual intervention is required.

### Updating the Substack export

The subscriber export (`fct__substack_subscriber_daily`) depends on manual CSV uploads from Substack's dashboard. Each upload creates a new snapshot date in the data. If the export is not refreshed, subscriber counts will be stale. The refresh cadence should be defined and monitored as an SLA.

### Known data quality considerations

**`billing_interval` fallback logic:** For iOS/Android subscribers who don't have a Stripe subscription, billing interval is inferred from the plan name string or a $50 price threshold. This is an approximation and may misclassify edge cases (e.g., discounted annual plans under $50).

**Non-Stripe paid subscribers (`is_non_stripe_paid`):** These subscribers have a `first_payment_at` timestamp from Substack but no matching Stripe subscription. They are Apple or Google in-app purchasers. Their revenue appears at the publication level in the App Store payout stream, not at the individual subscriber level.

**Email matching:** The Stripe-Substack join is done on email. If a subscriber uses different email addresses across the two systems, they will appear as unmatched in the subscriber daily model (`stripe_subscription_id` will be null). This affects a small number of subscribers and does not affect the revenue accrual models, which are Stripe-native.

**EWMA requires 5 months of history:** The projection model produces less reliable output in the early months of a new publication or after a major revenue event resets the baseline.
