# Metric Definitions

Business definitions used by the dbt models in `analytics/`. A metric is only published when its definition is written here.

## Order status

| Source (`orders.status`, `is_deleted`) | Business status | Meaning |
|---|---|---|
| `CREATED` | `pending` | Order placed, payment not confirmed yet |
| `PAID` | `successful` | Payment confirmed |
| any status with `is_deleted = TRUE` | `cancelled` | Order deleted in the source system |

Decisions:

- The source has no refund process. A deleted order is reported as **cancelled**, not as refunded.
- A source status that is not in this table makes the `fct_orders` build fail (`accepted_values` test). It is not published as `unknown`. The staging layer raises a warning first, so a new status is visible before it reaches the mart.

## Metrics (from `fct_orders`)

| Metric | Definition |
|---|---|
| Successful orders | `count(*) where order_status = 'successful'` |
| Pending orders | `count(*) where order_status = 'pending'` |
| Cancelled orders | `count(*) where order_status = 'cancelled'` |
| Successful order value | `sum(successful_amount)` |

All counts describe the **current state** of each order. Example: an order created on 20 November and paid on 22 November counts as successful, with `order_date` = 20 November.

## Daily status (from `fct_order_status_daily`)

| Metric | Definition |
|---|---|
| Pending orders at end of day D | `count(*) where status_date = D and order_status = 'pending'` |
| Successful orders at end of day D | same, with `order_status = 'successful'` |
| Cancelled orders at end of day D | same, with `order_status = 'cancelled'` |

"End of day" means 00:00 UTC of the next day. The status is the one in effect at that moment.

## Limits of the daily history

- The source keeps only the latest state of each order. History is recorded by `dbt snapshot` from its first run onward; earlier states cannot be rebuilt.
- A new version starts at the time of the change event (Debezium `ts_ms`), not at the time `dbt snapshot` ran.
- If an order changes more than once between two runs, only the last change is recorded. Example: CREATED → PAID → DELETED between two runs is recorded as CREATED → DELETED.
- An order appears in the daily table from the day of its first recorded version.
