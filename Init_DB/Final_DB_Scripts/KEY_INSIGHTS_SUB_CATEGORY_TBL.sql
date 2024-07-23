INSERT INTO POSTGRES_SCHEMA.KEY_INSIGHTS_SUB_CATEGORY_TABLE (
    sub_category,
    current_mtd_demand,
    prev_mtd_demand,
    gain,
    gain_percent,
    current_period,
    prev_period
)
WITH max_date AS (
    SELECT MAX(order_date::date) AS max_created_date
    FROM POSTGRES_SCHEMA.SUB_CATEGORY_TABLE
),
current_period AS (
    SELECT
        DATE_TRUNC('month', max_created_date)::date AS month_start,
        max_created_date::date AS month_end
    FROM max_date
),
prev_period AS (
    SELECT
        MAX((DATE_TRUNC('month', max_created_date) - INTERVAL '1 MONTH')::date) AS month_start,
        (DATE_TRUNC('month', MAX(max_created_date::date) - INTERVAL '1 MONTH') + 
            (MAX(max_created_date::date) - DATE_TRUNC('month', MAX(max_created_date::date))))::date AS month_end
    FROM max_date
),
current_mtd AS (
    SELECT
        cp.month_start,
        cp.month_end,
        sub_category,
        COALESCE(SUM(total_orders_delivered), 0) AS mtd_order_demand
    FROM 
        POSTGRES_SCHEMA.SUB_CATEGORY_TABLE
        CROSS JOIN current_period cp
    WHERE 
        order_date::date BETWEEN cp.month_start AND cp.month_end
    GROUP BY 
    cp.month_start, cp.month_end, sub_category
),
prev_mtd AS (
    SELECT
        pp.month_start,
        pp.month_end,
        sub_category,
        COALESCE(SUM(total_orders_delivered), 0) AS mtd_order_demand
    FROM 
        POSTGRES_SCHEMA.SUB_CATEGORY_TABLE
        CROSS JOIN prev_period pp
    WHERE 
        order_date::date BETWEEN pp.month_start AND pp.month_end
    GROUP BY 
        pp.month_start, pp.month_end, sub_category
),
subcat_demand AS (
    SELECT 
        COALESCE(c.sub_category, p.sub_category) AS sub_category,
        c.mtd_order_demand AS current_mtd_demand,
        p.mtd_order_demand AS prev_mtd_demand,
        COALESCE(c.mtd_order_demand, 0) - COALESCE(p.mtd_order_demand, 0) AS gain,
        CASE 
            WHEN p.mtd_order_demand IS NULL OR p.mtd_order_demand = 0 THEN
                100
            ELSE
                ROUND((COALESCE(c.mtd_order_demand, 0) - COALESCE(p.mtd_order_demand, 0)) * 100.0 / COALESCE(p.mtd_order_demand, 1), 2)
        END AS gain_percent,
        COALESCE(c.month_start, current_period.month_start) AS current_period_start,
        COALESCE(c.month_end, current_period.month_end) AS current_period_end,
        COALESCE(p.month_start, prev_period.month_start) AS prev_period_start,
        COALESCE(p.month_end, prev_period.month_end) AS prev_period_end
    FROM 
        current_mtd c
    FULL OUTER JOIN 
        prev_mtd p ON c.sub_category = p.sub_category
    CROSS JOIN current_period
    CROSS JOIN prev_period
)
SELECT 
    sub_category, 
    coalesce(current_mtd_demand,0) as  current_mtd_demand,
    coalesce(prev_mtd_demand,0) as prev_mtd_demand,
    coalesce(gain, 0) as gain, 
    coalesce(gain_percent, 0) as gain_percent,
    TO_CHAR(current_period_start, 'Mon DD, YYYY') || ' - ' || TO_CHAR(current_period_end, 'Mon DD, YYYY') as current_period,
    TO_CHAR(prev_period_start, 'Mon DD, YYYY') || ' - ' || TO_CHAR(prev_period_end, 'Mon DD, YYYY') as prev_period
FROM 
    subcat_demand
WHERE 
    sub_category <> ''
    AND sub_category IS NOT NULL
    AND UPPER(sub_category) <> 'UNDEFINED'
ORDER BY 
    gain_percent DESC;