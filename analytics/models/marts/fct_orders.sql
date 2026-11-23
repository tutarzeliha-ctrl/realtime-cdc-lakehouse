-- Grain: one row per order, current state.
-- Daily history is not available from the source (it keeps only the latest state);
-- it will be captured from now on with a dbt snapshot (next milestone).

select
    order_id,
    customer_name,
    order_status,
    source_status,
    is_deleted,
    order_amount,
    case when order_status = 'successful' then order_amount else 0 end as successful_amount,
    created_at_utc,
    cast(created_at_utc as date)    as order_date,
    last_changed_at_utc
from {{ ref('stg_orders') }}
