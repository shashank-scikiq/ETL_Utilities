INSERT INTO POSTGRES_SCHEMA.RV_DISTRICT_TBL
(SELECT
    order_date,
    delivery_district,
    delivery_state,
    delivery_state_code,
    COALESCE(COUNT(DISTINCT network_order_id),0) AS total_orders_delivered,
    COALESCE(sum(rvfod.total_items),0) as total_items
FROM POSTGRES_SCHEMA.AGG_TBL_VOUCHER rvfod 
group by order_date, delivery_district, delivery_state, delivery_state_code
ORDER BY order_date, delivery_district)