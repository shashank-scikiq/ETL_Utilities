INSERT INTO POSTGRES_SCHEMA.SUB_CATEGORY_PENETRATION_TBL
(order_date, category, sub_category, delivery_district, delivery_state, 
delivery_state_code, total_orders_delivered)
(select
	order_date,
	category,
	'ALL' as sub_category,
	delivery_district,
	delivery_state,
	delivery_state_code,
	sum(total_orders_delivered) as total_orders_delivered
	from POSTGRES_SCHEMA.CATEGORY_TBL
	group by order_date,category,sub_category, delivery_district,delivery_state, delivery_state_code
UNION
	select
	order_date,
	category,
	sub_category,
	delivery_district,
	delivery_state,
	delivery_state_code,
	sum(total_orders_delivered) as total_orders_delivered
	from POSTGRES_SCHEMA.SUB_CATEGORY_TBL
	group by order_date,category,sub_category, delivery_district,delivery_state, delivery_state_code)
