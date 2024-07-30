INSERT into POSTGRES_SCHEMA.B2B_DISTRICT_TBL(order_date,
 delivery_district, delivery_state,
 delivery_state_code, total_orders_delivered, total_items)
SELECT
    order_date,
    delivery_district,
    delivery_state,
    delivery_state_code,
    COALESCE(COUNT(DISTINCT network_order_id),0) AS total_orders_delivered,
    COALESCE(sum(total_items),0) as total_items
FROM POSTGRES_SCHEMA.AGG_TBL_B2B
group by order_date, delivery_district, delivery_state, delivery_state_code
ORDER BY order_date, delivery_district