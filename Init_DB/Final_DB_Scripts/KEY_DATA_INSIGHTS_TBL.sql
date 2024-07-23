INSERT INTO POSTGRES_SCHEMA.KEY_DATA_INSIGHTS_TBL(
    delivery_state,
    delivery_district,
    current_mtd_demand,
    prev_mtd_demand,
    current_wtd_demand,
    prev_mtd_intrastate,
    current_mtd_intrastate,
    current_mtd_intradistrict,
    prev_mtd_intradistrict,
    current_wtd_intrastate,
    current_wtd_intradistrict,
    prev_wtd_demand,
    prev_wtd_intrastate,
    prev_wtd_intradistrict,
    total_gain,
    intrastate_gain,
    intradistrict_gain,
    wtd_total_gain,
    wtd_intrastate_gain,
    wtd_intradistrict_gain,
    total_gain_percent,
    intrastate_gain_percent,
    intradistrict_gain_percent,
    wtd_total_gain_percent,
    wtd_intrastate_gain_percent,
    wtd_intradistrict_gain_percent,
    current_period,
    prev_period,
    current_wtd_period,
    prev_wtd_period
)
WITH max_date AS (
    SELECT 
        MAX(order_date::date) AS max_created_date,
        DATE_TRUNC('month', MAX(order_date::date))::date AS current_mtd_start,
        MAX(order_date::date)::date AS current_mtd_end,
        DATE_TRUNC('month', MAX(order_date::date) - INTERVAL '1 MONTH')::date AS prev_mtd_start,
        (DATE_TRUNC('month', MAX(order_date::date) - INTERVAL '1 MONTH') + 
            (MAX(order_date::date) - DATE_TRUNC('month', MAX(order_date::date))))::date AS prev_mtd_end,
        DATE_TRUNC('week', MAX(order_date::date))::date AS current_wtd_start,
        MAX(order_date::date)::date AS current_wtd_end,
        DATE_TRUNC('week', MAX(order_date::date) - INTERVAL '1 week')::date AS prev_wtd_start,
        (DATE_TRUNC('week', MAX(order_date::date) - INTERVAL '1 week') + 
            (MAX(order_date::date) - DATE_TRUNC('week', MAX(order_date::date))))::date AS prev_wtd_end
    FROM POSTGRES_SCHEMA.DISTRICT_TBL
),
current_mtd AS (
    SELECT 
        delivery_state,
        delivery_district,
        COALESCE(SUM(total_orders_delivered), 0) AS mtd_order_demand,
        COALESCE(SUM(intrastate_orders), 0) AS mtd_intrastate_orders,
        COALESCE(SUM(intradistrict_orders), 0) AS mtd_intradistrict_orders
    FROM 
        POSTGRES_SCHEMA.DISTRICT_TBL, max_date
    WHERE 
        order_date::date BETWEEN current_mtd_start AND current_mtd_end
    GROUP BY 
        delivery_state, delivery_district
),
prev_mtd AS (
    SELECT
        delivery_state,
        delivery_district,
        COALESCE(SUM(total_orders_delivered), 0) AS mtd_order_demand,
        COALESCE(SUM(intrastate_orders), 0) AS mtd_intrastate_orders,
        COALESCE(SUM(intradistrict_orders), 0) AS mtd_intradistrict_orders
    FROM 
        POSTGRES_SCHEMA.DISTRICT_TBL, max_date
    WHERE 
        order_date::date BETWEEN prev_mtd_start AND prev_mtd_end
    GROUP BY 
        delivery_state, delivery_district
),
current_wtd AS (
    SELECT
        delivery_state,
        delivery_district,
        COALESCE(SUM(total_orders_delivered), 0) AS wtd_order_demand,
        COALESCE(SUM(intrastate_orders), 0) AS wtd_intrastate_orders,
        COALESCE(SUM(intradistrict_orders), 0) AS wtd_intradistrict_orders
    FROM 
        POSTGRES_SCHEMA.DISTRICT_TBL, max_date
    WHERE 
        order_date::date BETWEEN current_wtd_start AND current_wtd_end
    GROUP BY 
        delivery_state, delivery_district
),
prev_wtd AS (
    SELECT
        delivery_state,
        delivery_district,
        COALESCE(SUM(total_orders_delivered), 0) AS wtd_order_demand,
        COALESCE(SUM(intrastate_orders), 0) AS wtd_intrastate_orders,
        COALESCE(SUM(intradistrict_orders), 0) AS wtd_intradistrict_orders
    FROM 
        POSTGRES_SCHEMA.DISTRICT_TBL, max_date
    WHERE 
        order_date::date BETWEEN prev_wtd_start AND prev_wtd_end
    GROUP BY 
        delivery_state, delivery_district
)
SELECT 
    COALESCE(c.delivery_state, p.delivery_state, cw.delivery_state, pw.delivery_state) AS delivery_state,
    COALESCE(c.delivery_district, p.delivery_district, cw.delivery_district, pw.delivery_district) AS delivery_district,
    COALESCE(c.mtd_order_demand, 0) AS current_mtd_demand,
    COALESCE(p.mtd_order_demand, 0) AS prev_mtd_demand,
    COALESCE(cw.wtd_order_demand, 0) AS current_wtd_demand,
    COALESCE(p.mtd_intrastate_orders, 0) AS prev_mtd_intrastate,
    COALESCE(c.mtd_intrastate_orders, 0) AS current_mtd_intrastate,
    COALESCE(c.mtd_intradistrict_orders, 0) AS current_mtd_intradistrict,
    COALESCE(p.mtd_intradistrict_orders, 0) AS prev_mtd_intradistrict,
    COALESCE(cw.wtd_intrastate_orders, 0) AS current_wtd_intrastate,
    COALESCE(cw.wtd_intradistrict_orders, 0) AS current_wtd_intradistrict,
    COALESCE(pw.wtd_order_demand, 0) AS prev_wtd_demand,
    COALESCE(pw.wtd_intrastate_orders, 0) AS prev_wtd_intrastate,
    COALESCE(pw.wtd_intradistrict_orders, 0) AS prev_wtd_intradistrict,
    COALESCE(c.mtd_order_demand, 0) - COALESCE(p.mtd_order_demand, 0) AS total_gain,
    COALESCE(c.mtd_intrastate_orders, 0) - COALESCE(p.mtd_intrastate_orders, 0) AS intrastate_gain,
    COALESCE(c.mtd_intradistrict_orders, 0) - COALESCE(p.mtd_intradistrict_orders, 0) AS intradistrict_gain,
    COALESCE(cw.wtd_order_demand, 0) - COALESCE(pw.wtd_order_demand, 0) AS wtd_total_gain,
    COALESCE(cw.wtd_intrastate_orders, 0) - COALESCE(pw.wtd_intrastate_orders, 0) AS wtd_intrastate_gain,
    COALESCE(cw.wtd_intradistrict_orders, 0) - COALESCE(pw.wtd_intradistrict_orders, 0) AS wtd_intradistrict_gain,
    CASE
        WHEN COALESCE(p.mtd_order_demand, 0) = 0 THEN
            100.0
        ELSE
            ROUND(100.0 * (COALESCE(c.mtd_order_demand, 0) - COALESCE(p.mtd_order_demand, 0)) / NULLIF(p.mtd_order_demand, 0), 2)
    END AS total_gain_percent,
    CASE
        WHEN COALESCE(p.mtd_intrastate_orders, 0) = 0 THEN
            100.0
        ELSE
            ROUND(100.0 * (COALESCE(c.mtd_intrastate_orders, 0) - COALESCE(p.mtd_intrastate_orders, 0)) / NULLIF(p.mtd_intrastate_orders, 0), 2)
    END AS intrastate_gain_percent,
    CASE
        WHEN COALESCE(p.mtd_intradistrict_orders, 0) = 0 THEN
            100.0
        ELSE
            ROUND(100.0 * (COALESCE(c.mtd_intradistrict_orders, 0) - COALESCE(p.mtd_intradistrict_orders, 0)) / NULLIF(p.mtd_intradistrict_orders, 0), 2)
    END AS intradistrict_gain_percent,
    CASE
        WHEN COALESCE(pw.wtd_order_demand, 0) = 0 THEN
            100.0
        ELSE
            ROUND(100.0 * (COALESCE(cw.wtd_order_demand, 0) - COALESCE(pw.wtd_order_demand, 0)) / NULLIF(pw.wtd_order_demand, 0), 2)
    END AS wtd_total_gain_percent,
    CASE
        WHEN COALESCE(pw.wtd_intrastate_orders, 0) = 0 THEN
            100.0
        ELSE
            ROUND(100.0 * (COALESCE(cw.wtd_intrastate_orders, 0) - COALESCE(pw.wtd_intrastate_orders, 0)) / NULLIF(pw.wtd_intrastate_orders, 0), 2)
    END AS wtd_intrastate_gain_percent,
    CASE
        WHEN COALESCE(pw.wtd_intradistrict_orders, 0) = 0 THEN
            100.0
        ELSE
            ROUND(100.0 * (COALESCE(cw.wtd_intradistrict_orders, 0) - COALESCE(pw.wtd_intradistrict_orders, 0)) / NULLIF(pw.wtd_intradistrict_orders, 0), 2)
    END AS wtd_intradistrict_gain_percent,
    TO_CHAR(md.current_mtd_start, 'Mon DD, YYYY') || ' to ' || TO_CHAR(md.current_mtd_end, 'Mon DD, YYYY') AS current_period,
    TO_CHAR(md.prev_mtd_start, 'Mon DD, YYYY') || ' to ' || TO_CHAR(md.prev_mtd_end, 'Mon DD, YYYY') AS prev_period,
    TO_CHAR(md.current_wtd_start, 'Mon DD, YYYY') || ' to ' || TO_CHAR(md.current_wtd_end, 'Mon DD, YYYY') AS current_wtd_period,
    TO_CHAR(md.prev_wtd_start, 'Mon DD, YYYY') || ' to ' || TO_CHAR(md.prev_wtd_end, 'Mon DD, YYYY') AS prev_wtd_period
FROM current_mtd c
FULL OUTER JOIN prev_mtd p ON c.delivery_state = p.delivery_state AND c.delivery_district = p.delivery_district
FULL OUTER JOIN current_wtd cw ON c.delivery_state = cw.delivery_state AND c.delivery_district = cw.delivery_district
FULL OUTER JOIN prev_wtd pw ON c.delivery_state = pw.delivery_state AND c.delivery_district = pw.delivery_district
CROSS JOIN max_date md
ORDER BY total_gain_percent DESC, intrastate_gain_percent DESC, intradistrict_gain_percent DESC;