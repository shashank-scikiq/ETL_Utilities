INSERT INTO POSTGRES_SCHEMA.SUB_CATEGORY_TBL (
    order_date,
    category,
    sub_category,
    delivery_district,
    delivery_state_code,
    delivery_state,
    seller_district,
    seller_state_code,
    seller_state,
    total_orders_delivered,
    pincode_count,
    total_items,
    intrastate_orders,
    interstate_orders,
    intradistrict_orders,
    interdistrict_orders,
    intrastate_orders_percentage,
    intradistrict_orders_percentage
)
WITH DetailedOrders AS (
    SELECT
        order_date,
        consolidated_categories AS category,
        sub_category,
        delivery_district,
        delivery_state AS delivery_state,
        delivery_state_code,
        seller_state AS seller_state,
        seller_district,
        seller_state_code,
        network_order_id,
        delivery_pincode,
        total_items
    FROM
        POSTGRES_SCHEMA.AGG_TBL_B2C
),
AggregatedOrders AS (
    SELECT
        order_date,
        category,
        sub_category,
        delivery_district,
        delivery_state_code,
        MAX(delivery_state) AS delivery_state,
        seller_district,
        seller_state_code,
        MAX(seller_state) AS seller_state,
        COALESCE(COUNT(DISTINCT network_order_id),0) AS total_orders_delivered,
        COALESCE(COUNT(DISTINCT delivery_pincode),0) AS pincode_count,
        COALESCE(SUM(total_items),0) AS total_items,
        COALESCE(SUM(CASE WHEN delivery_state = seller_state THEN 1 ELSE 0 END),0) AS intrastate_orders,
        COALESCE(SUM(CASE WHEN delivery_state <> seller_state THEN 1 ELSE 0 END),0) AS interstate_orders,
        COALESCE(SUM(CASE WHEN delivery_district = seller_district THEN 1 ELSE 0 END),0) AS intradistrict_orders,
        COALESCE(SUM(CASE WHEN delivery_district <> seller_district THEN 1 ELSE 0 END),0) AS interdistrict_orders
    FROM
        DetailedOrders
    GROUP BY
         order_date, category, sub_category, delivery_district, delivery_state_code, seller_district, seller_state_code
)
SELECT
    order_date,
    category,
    sub_category,
    delivery_district,
    delivery_state_code,
    delivery_state,
    seller_district,
    seller_state_code,
    seller_state,
    total_orders_delivered,
    pincode_count,
    total_items,
    intrastate_orders,
    interstate_orders,
    intradistrict_orders,
    interdistrict_orders,
    CASE WHEN total_orders_delivered > 0 THEN
        ROUND(100.0 * intrastate_orders / COALESCE(total_orders_delivered,1), 2)
    ELSE 0 END AS intrastate_orders_percentage,
    CASE WHEN total_orders_delivered > 0 THEN
        ROUND(100.0 * intradistrict_orders / COALESCE(total_orders_delivered,1), 2)
    ELSE 0 END AS intradistrict_orders_percentage
FROM
    AggregatedOrders
ORDER BY
    order_date, category, sub_category;