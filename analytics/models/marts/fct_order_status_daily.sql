-- Grain: one row per order per day, with the order's status at the end of that day (UTC).
--
-- Built from the snapshot history. An order appears from the day of its first recorded
-- version: days before that are not known and are not filled in.

{% set as_of_date = var('as_of_date', none) %}

with versions as (

    select
        order_id,
        order_status,
        order_amount,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('orders_status_history') }}

),

days as (

    select cast(day as date) as status_date
    from generate_series(
        (select min(cast(dbt_valid_from as date)) from versions),
        {% if as_of_date %} date '{{ as_of_date }}' {% else %} current_date {% endif %},
        interval 1 day
    ) as t(day)

),

end_of_day_status as (

    -- The version in effect at the end of the day: started before midnight and
    -- not replaced before midnight.
    select
        d.status_date,
        v.order_id,
        v.order_status,
        v.order_amount
    from days d
    join versions v
      on v.dbt_valid_from < d.status_date + interval 1 day
     and (v.dbt_valid_to is null or v.dbt_valid_to >= d.status_date + interval 1 day)

)

select
    status_date,
    order_id,
    order_status,
    order_amount,
    case when order_status = 'successful' then order_amount else 0 end as successful_amount
from end_of_day_status
