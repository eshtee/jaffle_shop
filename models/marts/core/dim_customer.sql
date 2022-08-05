{{ config (
    materialized = "table"
) }}

WITH customers AS (

    SELECT
        *
    FROM
        {{ ref('stg_customers') }}
),
orders AS (
    SELECT
        *
    FROM
        {{ ref('stg_orders') }}
),
customer_orders AS (
    SELECT
        *
    FROM
        {{ ref('stg_customer_orders') }}
),
customer_lifetime_value AS (
    SELECT
        *
    FROM
        {{ ref('stg_customer_lifetime_value') }}
),
FINAL AS (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        COALESCE(
            customer_orders.number_of_orders,
            0
        ) AS number_of_orders,
        COALESCE(
            customer_lifetime_value.lifetime_value,
            0
        ) AS lifetime_value
    FROM
        customers
        LEFT JOIN customer_orders USING (customer_id)
        left join customer_lifetime_value using (customer_id)
)
SELECT
    *
FROM
    FINAL
