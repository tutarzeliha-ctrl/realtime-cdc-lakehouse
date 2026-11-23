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

## What these metrics cannot answer yet

"How many orders were pending at the end of 20 November?" The source keeps only the latest state of each order, so past states cannot be rebuilt. Daily history will be captured from the next milestone onward with a dbt snapshot.
