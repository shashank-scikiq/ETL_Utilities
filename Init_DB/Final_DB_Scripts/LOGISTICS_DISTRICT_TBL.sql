INSERT INTO POSTGRES_SCHEMA.LOGISTICS_DISTRICT_TBL
(order_date, delivery_district, delivery_state, delivery_state_code, seller_state, 
seller_district, seller_state_code, total_orders_delivered, pincode_count, 
intrastate_orders, intradistrict_orders, interstate_orders, intrastate_orders_percentage, 
intradistrict_orders_percentage)
WITH OrderLevelAggregation AS (
	    SELECT
	        order_date,
	        delivery_district,
	        delivery_state,
	        delivery_state_code,
	        seller_state,
	        seller_district,
	        seller_state_code,
	        concat(order_id, transaction_id, bap_id, bpp_id, cast(order_date as varchar)) as network_order_id,
	        COALESCE(COUNT(DISTINCT delivery_pincode),0) AS pincode_count
	    FROM POSTGRES_SCHEMA.AGG_TBL_LOG a
	    GROUP BY
	         order_date, delivery_district, delivery_state,
	        delivery_state_code, seller_state, seller_district, seller_state_code,
	        concat(order_id, transaction_id, bap_id, bpp_id, cast(order_date as varchar))
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
	        COALESCE(COUNT(distinct network_order_id),0) AS total_orders_delivered,
	        COALESCE(SUM(pincode_count),0) AS pincode_count,
	        COALESCE(SUM(CASE WHEN delivery_state = seller_state THEN 1 ELSE 0 END),0) AS intrastate_orders,
	        COALESCE(SUM(CASE WHEN delivery_state <> seller_state THEN 1 ELSE 0 END),0) AS interstate_orders,
	        COALESCE(SUM(CASE WHEN delivery_district = seller_district THEN 1 ELSE 0 END),0) AS intradistrict_orders,
	        COALESCE(SUM(CASE WHEN delivery_district <> seller_district THEN 1 ELSE 0 END),0) AS interdistrict_orders
	    FROM OrderLevelAggregation
	    GROUP BY
	         order_date, delivery_district, delivery_state_code, seller_district, seller_state_code
	)
	SELECT
    order_date,
    delivery_district,
    MAX(delivery_state) AS delivery_state,
    delivery_state_code,
    MAX(seller_state) AS seller_state,
    seller_district,
    seller_state_code,
    COALESCE(SUM(total_orders_delivered),0) AS total_orders_delivered,
    COALESCE(SUM(pincode_count),0) AS pincode_count,
    COALESCE(SUM(intrastate_orders),0) AS intrastate_orders,
    COALESCE(SUM(intradistrict_orders),0) AS intradistrict_orders,
    COALESCE(SUM(interstate_orders),0) AS interstate_orders,
    ROUND(100.0 * SUM(intrastate_orders) / COALESCE(NULLIF(SUM(total_orders_delivered), 0), 1), 2) AS intrastate_orders_percentage,
    ROUND(100.0 * SUM(intradistrict_orders) / COALESCE(NULLIF(SUM(total_orders_delivered), 0), 1), 2) AS intradistrict_orders_percentage
FROM
    IntraOrdersAggregation
GROUP BY
     order_date,
     delivery_district, delivery_state_code, seller_district, seller_state_code
ORDER BY
    order_date