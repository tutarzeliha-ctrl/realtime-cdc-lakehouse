-- Reconciliation: on the last day, the daily table must agree with fct_orders
-- (the current state). Returns the orders that differ.
-- Skipped when as_of_date points to a past day.

{% if var('as_of_date', none) is none %}

with last_day as (
    select *
    from {{ ref('fct_order_status_daily') }}
    where status_date = (select max(status_date) from {{ ref('fct_order_status_daily') }})
)

select
    coalesce(c.order_id, d.order_id) as order_id,
    c.order_status as current_status,
    d.order_status as daily_status
from {{ ref('fct_orders') }} c
full outer join last_day d on c.order_id = d.order_id
where c.order_id is null
   or d.order_id is null
   or c.order_status <> d.order_status

{% else %}

select 1 where false

{% endif %}
