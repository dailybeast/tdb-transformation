WITH base AS (
  SELECT
    'charge' AS record_type,
    c.id,
    c.created AS created_at,
    c.description,
    c.status,

    c.amount / 100.0 AS amount,
    LOWER(c.currency) AS currency,

    c.balance_transaction_id,
    c.customer_id,
    c.receipt_email AS customer_email,

    bt.amount / 100.0 AS bt_amount,
    LOWER(bt.currency) AS bt_currency,
    bt.fee / 100.0 AS bt_fee

  FROM `ai-mvp-392019.stripe.charge` c
  LEFT JOIN `ai-mvp-392019.stripe.balance_transaction` bt
    ON bt.id = c.balance_transaction_id
  WHERE c.status = 'succeeded'
),

normalized AS (
  SELECT
    *,
    CASE
      WHEN bt_amount IS NOT NULL AND bt_currency = 'usd' THEN bt_amount
      WHEN bt_amount IS NULL AND currency = 'usd' THEN amount
      ELSE bt_amount
    END AS converted_amount_usd,

    CASE
      WHEN bt_fee IS NOT NULL AND bt_currency = 'usd' THEN bt_fee
      WHEN bt_fee IS NULL AND currency = 'usd' THEN 0.0
      ELSE bt_fee
    END AS stripe_fee_usd
  FROM base
),

classified AS (
  SELECT
    *,

    CASE
      WHEN converted_amount_usd < 10 THEN 'monthly'
      ELSE 'annual'
    END AS billing_frequency,

    DATE_TRUNC(
      CASE
        WHEN EXTRACT(DAY FROM DATE(created_at)) >= 16 THEN DATE_ADD(DATE(created_at), INTERVAL 1 MONTH)
        ELSE DATE(created_at)
      END,
      MONTH
    ) AS accounting_month

  FROM normalized
  WHERE converted_amount_usd IS NOT NULL
)

SELECT
  accounting_month,

  billing_frequency,
  record_type,
  id,
  created_at,
  description,

  amount,
  currency,

  converted_amount_usd,
  'usd' AS converted_currency,

  stripe_fee_usd,
  0.0 AS substack_fee_usd,
  (COALESCE(stripe_fee_usd, 0.0) + 0.0) AS total_fees_usd,

  (converted_amount_usd - (COALESCE(stripe_fee_usd, 0.0) + 0.0)) AS net_total_usd,

  CASE
    WHEN billing_frequency = 'monthly' THEN (converted_amount_usd - (COALESCE(stripe_fee_usd, 0.0) + 0.0))
    ELSE ROUND((converted_amount_usd - (COALESCE(stripe_fee_usd, 0.0) + 0.0)) / 12.0, 2)
  END AS recognized_net_usd,

  CASE
    WHEN billing_frequency = 'monthly' THEN 0.0
    ELSE ROUND(
      (converted_amount_usd - (COALESCE(stripe_fee_usd, 0.0) + 0.0))
      - ((converted_amount_usd - (COALESCE(stripe_fee_usd, 0.0) + 0.0)) / 12.0),
      2
    )
  END AS deferred_net_usd_initial,

  CAST(NULL AS FLOAT64) AS tax_liability_usd,

  customer_id,
  customer_email,
  balance_transaction_id
FROM classified