INSERT INTO POSTGRES_SCHEMA.LOG_CATEGORY_TBL
(order_date, category, delivery_district, delivery_state, delivery_state_code, 
seller_state, seller_district, seller_state_code, total_orders_delivered, pincode_count, 
total_items, intrastate_orders, intradistrict_orders, interstate_orders, intrastate_orders_percentage, 
intradistrict_orders_percentage)
WITH ItemLevelAggregation AS (
    SELECT
        order_date,
        network_retail_order_category  AS category,
        delivery_district,
        delivery_state,
        delivery_state_code,
        seller_state,
        seller_district,
        seller_state_code,
        network_retail_order_id as network_order_id,
        MAX(delivery_pincode) AS delivery_pincode,
        COALESCE(count(distinct item_id),0) AS total_items
    FROM POSTGRES_SCHEMA.AGG_TBL_LOG
    GROUP BY
         order_date,
         category,
         delivery_district, delivery_state,
        delivery_state_code, seller_state, seller_district, seller_state_code,
        network_retail_order_id
),OrderLevelAggregation AS (
    SELECT
        order_date,
        category,
        delivery_district,
        delivery_state,
        delivery_state_code,
        seller_state,
        seller_district,
        seller_state_code,
        COALESCE(COUNT(DISTINCT network_order_id),0) AS total_orders_delivered,
        COALESCE(COUNT(distinct delivery_pincode),0) AS pincode_count,
        COALESCE(SUM(total_items),0) AS total_items,
        COALESCE(SUM(CASE WHEN delivery_state = seller_state THEN 1 ELSE 0 END),0) AS intrastate_orders,
        COALESCE(SUM(CASE WHEN delivery_district = seller_district THEN 1 ELSE 0 END),0) AS intradistrict_orders,
        COALESCE(SUM(CASE WHEN delivery_state != seller_state THEN 1 ELSE 0 END),0) AS interstate_orders
    FROM
        ItemLevelAggregation
    GROUP BY
         order_date,
         category,
         delivery_district, delivery_state, delivery_state_code, seller_state, seller_district, seller_state_code
)
SELECT
    order_date,
    category,
    delivery_district,
    MAX(delivery_state) AS delivery_state,
    delivery_state_code,
    MAX(seller_state) AS seller_state,
    seller_district,
    seller_state_code,
    COALESCE(SUM(total_orders_delivered),0) AS total_orders_delivered,
    COALESCE(SUM(pincode_count),0) AS pincode_count,
    COALESCE(SUM(total_items),0) AS total_items,
    COALESCE(SUM(intrastate_orders),0) AS intrastate_orders,
    COALESCE(SUM(intradistrict_orders),0) AS intradistrict_orders,
    COALESCE(SUM(interstate_orders),0) AS interstate_orders,
    ROUND(100.0 * SUM(intrastate_orders) / COALESCE(NULLIF(SUM(total_orders_delivered), 0), 1), 2) AS intrastate_orders_percentage,
    ROUND(100.0 * SUM(intradistrict_orders) / COALESCE(NULLIF(SUM(total_orders_delivered), 0), 1), 2) AS intradistrict_orders_percentage
FROM
    OrderLevelAggregation
GROUP BY
     order_date,
     category,
     delivery_district, delivery_state_code, seller_district, seller_state_code
ORDER BY
    order_date,
    category;