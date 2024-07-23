INSERT INTO POSTGRES_SCHEMA.AGG_TBL_B2B(network_order_id,seller_np, 
total_items, "domain",provider_key, order_status, delivery_pincode,
order_date, delivery_state, delivery_state_code, delivery_district)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)