-- Reconciliation: fct_orders must contain every order from the source snapshot
-- and the same total amount. Returns a row (= failure) when they differ.

with source_totals as (
    select count(*) as row_count, sum(amount) as total_amount
    from {{ source('cdc', 'orders_snapshot') }}
),

mart_totals as (
    select count(*) as row_count, sum(order_amount) as total_amount
    from {{ ref('fct_orders') }}
)

select
    s.row_count    as source_rows,
    m.row_count    as mart_rows,
    s.total_amount as source_amount,
    m.total_amount as mart_amount
from source_totals s
cross join mart_totals m
where s.row_count <> m.row_count
   or s.total_amount <> m.total_amount
