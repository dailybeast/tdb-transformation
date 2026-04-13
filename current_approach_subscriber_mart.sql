WITH max_day AS (
  SELECT MAX(snapshot_date) AS last_day
  FROM `ai-mvp-392019.substack.royalist_daily_snapshot`
),

daily_status AS (
  SELECT
    s.snapshot_date,
    s.email,
    s.name,
    s.type,
    s.start_date,
    s.first_paid_date,
    s.cancel_date,
    s.expiration_date,

    CASE
      WHEN LOWER(s.type) = 'monthly gift' THEN 'Monthly Gift'
      WHEN LOWER(s.type) = 'yearly gift' THEN 'Yearly Gift'
      WHEN LOWER(s.type) = 'free' AND s.first_paid_date IS NOT NULL THEN
        CASE
          WHEN SAFE_CAST(s.revenue AS FLOAT64) >= 50 THEN 'Yearly Subscriber'
          WHEN SAFE_CAST(s.revenue AS FLOAT64) > 0   THEN 'Monthly Subscriber'
          ELSE 'Yearly Subscriber'
        END
      WHEN LOWER(s.type) LIKE '%monthly%' THEN 'Monthly Subscriber'
      WHEN LOWER(s.type) LIKE '%yearly%'  THEN 'Yearly Subscriber'
      WHEN LOWER(s.type) = 'subscriber'   THEN 'Monthly Subscriber'
      WHEN LOWER(s.type) LIKE '%royal%'   THEN 'Royal Tier'
      WHEN LOWER(s.type) LIKE '%comp%'    THEN
        CASE
          WHEN SAFE_CAST(s.revenue AS FLOAT64) >= 50 THEN 'Yearly Subscriber'
          WHEN SAFE_CAST(s.revenue AS FLOAT64) > 0   THEN 'Monthly Subscriber'
          ELSE 'Comp'
        END
      ELSE 'Other'
    END AS type_bucket,

    CASE
      WHEN LOWER(s.type) IN ('monthly gift','yearly gift') THEN
        CASE
          WHEN s.expiration_date IS NOT NULL
           AND s.expiration_date > TIMESTAMP(s.snapshot_date, 'America/New_York')
          THEN 'Active' ELSE 'Expired' END

      WHEN s.first_paid_date IS NULL THEN 'Non-paid'

      WHEN s.cancel_date IS NOT NULL THEN
        CASE
          WHEN s.expiration_date IS NOT NULL
           AND s.expiration_date > TIMESTAMP(s.snapshot_date, 'America/New_York')
          THEN 'Cancelled but Active' ELSE 'Expired' END

      ELSE
        CASE
          WHEN s.expiration_date IS NULL THEN 'Expired'
          WHEN s.expiration_date > TIMESTAMP(s.snapshot_date, 'America/New_York') THEN 'Active'
          ELSE 'Expired'
        END
    END AS status_bucket
  FROM `ai-mvp-392019.substack.royalist_daily_snapshot` s
),

daily_paid_flag AS (
  SELECT
    ds.*,
    ds.status_bucket IN ('Active','Cancelled but Active') AS is_active_paid
  FROM daily_status ds
),

status_on_last_day AS (
  SELECT
    d.email,
    ANY_VALUE(d.name) AS name_last,
    ANY_VALUE(d.type) AS type_last,
    ANY_VALUE(d.type_bucket) AS type_bucket_last,
    ANY_VALUE(d.status_bucket) AS status_bucket_last,
    ANY_VALUE(d.is_active_paid) AS is_active_paid_last
  FROM daily_paid_flag d
  JOIN max_day m
    ON d.snapshot_date = m.last_day
  GROUP BY d.email
),

latest_observed AS (
  SELECT
    email,
    ANY_VALUE(name) AS name_last_observed,
    ANY_VALUE(type) AS type_last_observed,
    ANY_VALUE(type_bucket) AS type_bucket_last_observed,
    ANY_VALUE(status_bucket) AS status_bucket_last_observed,
    ANY_VALUE(is_active_paid) AS is_active_paid_last_observed
  FROM (
    SELECT
      d.*,
      ROW_NUMBER() OVER (PARTITION BY d.email ORDER BY d.snapshot_date DESC) AS rn
    FROM daily_paid_flag d
  )
  WHERE rn = 1
  GROUP BY email
),

agg AS (
  SELECT
    email,
    MIN(snapshot_date) AS first_seen_date,
    MAX(snapshot_date) AS last_seen_date,
    MIN(start_date) AS signup_ts,
    MIN(first_paid_date) AS first_paid_ts,
    MIN(cancel_date) AS cancel_ts,
    MAX(IF(is_active_paid, snapshot_date, NULL)) AS last_active_paid_date
  FROM daily_paid_flag
  GROUP BY email
),

first_paid_type AS (
  SELECT
    email,
    ANY_VALUE(type_bucket) AS first_type_bucket
  FROM (
    SELECT
      email,
      type_bucket,
      ROW_NUMBER() OVER (PARTITION BY email ORDER BY snapshot_date ASC) AS rn
    FROM daily_paid_flag
    WHERE is_active_paid
  )
  WHERE rn = 1
  GROUP BY email
),

last_paid_type AS (
  SELECT
    email,
    ANY_VALUE(type_bucket) AS last_paid_type_bucket
  FROM (
    SELECT
      email,
      type_bucket,
      ROW_NUMBER() OVER (PARTITION BY email ORDER BY snapshot_date DESC) AS rn
    FROM daily_paid_flag
    WHERE is_active_paid
  )
  WHERE rn = 1
  GROUP BY email
),

emails AS (
  SELECT
    a.email,
    LOWER(TRIM(a.email)) AS email_norm
  FROM agg a
),

src_90_day_comp AS (
  SELECT DISTINCT LOWER(TRIM(string_field_0)) AS email_norm
  FROM `ai-mvp-392019.substack.90_day_comp`
),

src_imported_free AS (
  SELECT DISTINCT LOWER(TRIM(string_field_0)) AS email_norm
  FROM `ai-mvp-392019.substack.imported_free`
),

src_one_year_comp AS (
  SELECT DISTINCT LOWER(TRIM(string_field_0)) AS email_norm
  FROM `ai-mvp-392019.substack.oneyear_comp`
)

SELECT
  a.email,
  COALESCE(s.name_last, lo.name_last_observed) AS name,
  COALESCE(s.type_last, lo.type_last_observed) AS type,
  COALESCE(s.type_bucket_last, lo.type_bucket_last_observed, 'Other') AS type_bucket,

  fpt.first_type_bucket,
  COALESCE(
    lpt.last_paid_type_bucket,
    lo.type_bucket_last_observed,
    'Other'
  ) AS last_paid_type_bucket,

  a.first_seen_date,
  a.last_seen_date,

  COALESCE(DATE(a.signup_ts), a.first_seen_date) AS signup_date,
  DATE(a.first_paid_ts, 'America/New_York') AS first_paid_date,
  DATE(a.cancel_ts, 'America/New_York') AS cancel_date,

  COALESCE(s.status_bucket_last, 'Non-paid') AS current_status_bucket,
  COALESCE(s.is_active_paid_last, FALSE) AS is_currently_active_paid,

  CASE WHEN s.is_active_paid_last THEN NULL ELSE a.last_active_paid_date END AS paid_end_date,

  CASE
    WHEN s.is_active_paid_last THEN NULL
    WHEN a.last_active_paid_date IS NULL THEN NULL
    ELSE DATE_ADD(a.last_active_paid_date, INTERVAL 1 DAY)
  END AS churn_date,

  (a.last_seen_date < (SELECT last_day FROM max_day)) AS unsubscribed,

  CASE
    WHEN a.last_seen_date < (SELECT last_day FROM max_day)
    THEN DATE_ADD(a.last_seen_date, INTERVAL 1 DAY)
    ELSE NULL
  END AS unsubscribed_date,

  (c90.email_norm IS NOT NULL) AS is_90_day_comp,
  (imf.email_norm IS NOT NULL) AS is_imported_free,
  (c1y.email_norm IS NOT NULL) AS is_one_year_comp,

  CASE
    WHEN c90.email_norm IS NOT NULL THEN '90_day_comp'
    WHEN c1y.email_norm IS NOT NULL THEN 'oneyear_comp'
    WHEN imf.email_norm IS NOT NULL THEN 'imported_free'
    ELSE NULL
  END AS import_source

FROM agg a
LEFT JOIN status_on_last_day s USING (email)
LEFT JOIN latest_observed lo USING (email)
LEFT JOIN first_paid_type fpt USING (email)
LEFT JOIN last_paid_type  lpt USING (email)
LEFT JOIN emails e
  ON e.email = a.email
LEFT JOIN src_90_day_comp c90
  ON c90.email_norm = e.email_norm
LEFT JOIN src_imported_free imf
  ON imf.email_norm = e.email_norm
LEFT JOIN src_one_year_comp c1y
  ON c1y.email_norm = e.email_norm