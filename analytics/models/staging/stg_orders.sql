-- Grain: one row per order (current state, as published by the CDC consumer).

with source as (

    select * from {{ source('cdc', 'orders_snapshot') }}

),

typed as (

    select
        cast(order_id as bigint)            as order_id,
        customer_name,
        cast(amount as decimal(18, 3))      as order_amount,
        upper(trim(status))                 as source_status,
        is_deleted,

        -- created_at is ISO-8601 text, except for delete placeholders, which carry epoch ms.
        case
            when regexp_full_match(created_at, '[0-9]+')
                then to_timestamp(cast(created_at as bigint) / 1000.0)
            else cast(created_at as timestamptz)
        end                                 as created_at_tz,

        to_timestamp(cast(updated_at as bigint) / 1000.0) as last_changed_at_tz

    from source

)

select
    order_id,
    customer_name,
    order_amount,
    source_status,

    -- Business status (definitions in docs/METRICS.md)
    case
        when is_deleted               then 'cancelled'
        when source_status = 'PAID'    then 'successful'
        when source_status = 'CREATED' then 'pending'
        else 'unknown'
    end                                     as order_status,

    is_deleted,
    timezone('UTC', created_at_tz)          as created_at_utc,
    timezone('UTC', last_changed_at_tz)     as last_changed_at_utc

from typed
