{{ config(materialized='table') }}

with source as (
    select
        id                      as refund_id,
        charge_id,
        balance_transaction_id,
        created                 as refunded_at,
        status
    from {{ source('stripe', 'refund') }}
    where status = 'succeeded'
),

balance_transactions as (
    select
        id,
        abs(net) / 100.0        as refunded_net_usd
    from {{ source('stripe', 'balance_transaction') }}
)

select
    r.refund_id,
    r.charge_id,
    r.refunded_at,
    bt.refunded_net_usd
from source r
left join balance_transactions bt
    on bt.id = r.balance_transaction_id
