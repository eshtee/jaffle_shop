WITH FINAL AS (
    SELECT
        id,
        order_id,
        payment_method,
        amount / 100 AS amount
    FROM
        {{ source(
            'jaffle_shop',
            'raw_payments'
        ) }}
)
SELECT
    *
FROM
    FINAL
