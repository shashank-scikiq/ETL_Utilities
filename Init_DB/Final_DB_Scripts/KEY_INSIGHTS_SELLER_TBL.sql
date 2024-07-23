INSERT INTO POSTGRES_SCHEMA.KEY_INSIGHTS_SELLER (
    current_period,
    percentage_of_providers_meeting_80p
)
WITH max_date AS (
    SELECT
        MAX(order_date::date) AS period_end
    FROM POSTGRES_SCHEMA.AGG_TBL_B2C
), date_period AS (
    SELECT
        MIN(order_date::date) AS period_start,
        period_end
    FROM POSTGRES_SCHEMA.AGG_TBL_B2C
    CROSS JOIN max_date
    WHERE extract(year from order_date::date) = extract(year from max_date.period_end)
    AND extract(month from order_date::date) = extract(month from max_date.period_end)
    GROUP BY period_end
),
total_orders AS (
    SELECT COALESCE(count(DISTINCT network_order_id), 0) AS total
    FROM POSTGRES_SCHEMA.AGG_TBL_B2C
    CROSS JOIN max_date
    WHERE extract(year from order_date::date) = extract(year from max_date.period_end)
    AND extract(month from order_date::date) = extract(month from max_date.period_end)
),
ordered_providers AS (
    SELECT
        provider_key,
        count(DISTINCT network_order_id) AS provider_orders
    FROM
        POSTGRES_SCHEMA.AGG_TBL_B2C
    CROSS JOIN max_date
    WHERE extract(year from order_date::date) = extract(year from max_date.period_end)
    AND extract(month from order_date::date) = extract(month from max_date.period_end)
    GROUP BY
        provider_key
),
cumulative_orders AS (
    SELECT
        provider_key,
        provider_orders,
        SUM(provider_orders) OVER (ORDER BY provider_orders DESC) AS cumulative_sum
    FROM
        ordered_providers
),
providers_80p_threshold AS (
    SELECT
        provider_key
    FROM
        cumulative_orders
    WHERE
        cumulative_sum <= (SELECT 0.8 * total FROM total_orders)
),
provider_count AS (
    SELECT COALESCE(COUNT(DISTINCT provider_key), 0) AS total_providers
    FROM POSTGRES_SCHEMA.AGG_TBL_B2C
    CROSS JOIN max_date
    WHERE extract(year from order_date::date) = extract(year from max_date.period_end)
    AND extract(month from order_date::date) = extract(month from max_date.period_end)
),
providers_80p_count AS (
    SELECT COUNT(*) AS providers_meeting_80p
    FROM providers_80p_threshold
)
SELECT
    TO_CHAR(dp.period_start, 'Mon DD, YYYY') || ' - ' || TO_CHAR(dp.period_end, 'Mon DD, YYYY') as current_period,
    CASE
        WHEN pc.total_providers > 0 THEN
            (p80.providers_meeting_80p::float / pc.total_providers::float) * 100
        ELSE 0
    END AS percentage_of_providers_meeting_80p
FROM
    providers_80p_count p80
CROSS JOIN
    provider_count pc
CROSS JOIN
    date_period dp;