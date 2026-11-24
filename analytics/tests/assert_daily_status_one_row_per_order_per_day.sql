-- Grain check: each order may appear at most once per day.
-- Two rows would mean overlapping snapshot versions.

select status_date, order_id, count(*) as row_count
from {{ ref('fct_order_status_daily') }}
group by status_date, order_id
having count(*) > 1
