WITH orders AS (
    SELECT
        *
    FROM
        {{ ref('stg_orders') }}
),
payments AS (
    SELECT
        *
    FROM
        {{ ref('stg_payments') }}
),
FINAL AS (
    SELECT
        customer_id,
        SUM(amount) AS lifetime_value
    FROM
        orders
        LEFT JOIN payments USING (order_id)
    GROUP BY
        1
)
SELECT
    *
FROM
    FINAL
