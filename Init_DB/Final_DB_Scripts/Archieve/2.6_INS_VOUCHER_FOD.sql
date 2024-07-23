INSERT INTO POSTGRES_SCHEMA.AGG_TBL_VOUCHER
(network_order_id, provider_key, total_items, "domain", order_status, delivery_pincode, 
order_date, delivery_state, delivery_state_code, delivery_district)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)