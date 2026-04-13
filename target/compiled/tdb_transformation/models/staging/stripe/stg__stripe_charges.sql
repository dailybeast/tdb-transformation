

with

charges as (
    select
        id                      as charge_id,
        customer_id,
        invoice_id,
        balance_transaction_id,
        created                 as charged_at,
        amount / 100.0          as charge_amount,
        lower(currency)         as charge_currency,
        receipt_email,
        description,
        status
    from `ai-mvp-392019`.`stripe`.`charge`
    where status = 'succeeded'
),

balance_transactions as (
    select
        id,
        amount / 100.0          as settled_amount_usd,
        fee / 100.0             as stripe_fee_usd,
        net / 100.0             as net_amount_usd,
        exchange_rate
    from `ai-mvp-392019`.`stripe`.`balance_transaction`
),

joined as (
    select
        c.charge_id,
        c.customer_id,
        c.invoice_id,
        c.balance_transaction_id,
        c.charged_at,
        c.charge_amount,
        c.charge_currency,
        c.receipt_email,
        c.description,
        bt.settled_amount_usd,
        bt.stripe_fee_usd,
        bt.net_amount_usd,
        bt.exchange_rate
    from charges c
    left join balance_transactions bt
        on bt.id = c.balance_transaction_id
)

select * from joined