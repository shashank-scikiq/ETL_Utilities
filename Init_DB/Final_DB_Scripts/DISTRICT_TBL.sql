INSERT INTO POSTGRES_SCHEMA.DISTRICT_TBL
(
WITH OrderLevelAggregation AS (
    SELECT
        order_date,
        delivery_district,
        delivery_state,
        delivery_state_code,
        seller_district,
        seller_state,
        seller_state_code,
        network_order_id,
        COUNT(DISTINCT delivery_pincode) AS pincode_count,
        SUM(total_items) AS total_items
    FROM POSTGRES_SCHEMA.AGG_TBL_B2C swsclo
    GROUP BY
         order_date, delivery_district, delivery_state,
        delivery_state_code, seller_state, seller_district, seller_state_code,
        network_order_id
),
IntraOrdersAggregation AS (
    SELECT
        order_date,
        delivery_district,
        delivery_state_code,
        seller_district,
        seller_state_code,
        MAX(delivery_state) as delivery_state,
        MAX(seller_state) as seller_state,
        COUNT(network_order_id) AS total_orders_delivered,
        SUM(COALESCE(pincode_count, 0)) AS pincode_count,
        SUM(COALESCE(total_items, 0)) AS total_items,
        SUM(COALESCE(CASE WHEN delivery_state = seller_state THEN 1 ELSE 0 END, 0)) AS intrastate_orders,
        SUM(COALESCE(CASE WHEN delivery_state <> seller_state THEN 1 ELSE 0 END, 0)) AS interstate_orders,
        SUM(COALESCE(CASE WHEN delivery_district = seller_district THEN 1 ELSE 0 END, 0)) AS intradistrict_orders,
        SUM(COALESCE(CASE WHEN delivery_district <> seller_district THEN 1 ELSE 0 END, 0)) AS interdistrict_orders
    FROM OrderLevelAggregation
    GROUP BY
         order_date, delivery_district, delivery_state_code, seller_district, seller_state_code
)
SELECT
    order_date,
    delivery_district,
    delivery_state,
    delivery_state_code,
    seller_district,
    seller_state,
    seller_state_code,
    total_orders_delivered,
    pincode_count,
    total_items,
    intrastate_orders,
    interstate_orders,
    intradistrict_orders,
    interdistrict_orders,
    COALESCE(ROUND(100.0 * intrastate_orders / NULLIF(total_orders_delivered, 0), 2), 0) AS intrastate_orders_percentage,
    COALESCE(ROUND(100.0 * intradistrict_orders / NULLIF(total_orders_delivered, 0), 2), 0) AS intradistrict_orders_percentage
FROM IntraOrdersAggregation
ORDER BY order_date, delivery_district
);
