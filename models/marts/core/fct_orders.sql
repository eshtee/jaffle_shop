{{ config (
    materialized = "table"
) }}

WITH payments AS (

    SELECT
        *
    FROM
        {{ ref('stg_payments') }}
),
orders AS (
    SELECT
        *
    FROM
        {{ ref('stg_orders') }}
),
FINAL AS (
    SELECT
        orders.customer_id,
        orders.order_id,
        COALESCE(
            payments.amount,
            0
        ) AS amount
    FROM
        orders
        LEFT JOIN payments USING (order_id)
)
SELECT
    *
FROM
    FINAL
