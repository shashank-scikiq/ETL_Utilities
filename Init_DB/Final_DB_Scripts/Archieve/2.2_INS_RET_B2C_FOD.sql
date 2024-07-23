INSERT INTO POSTGRES_SCHEMA.AGG_TBL_B2C(network_order_id,sub_category,seller_np,total_items,"domain",
provider_key,order_status,seller_pincode,delivery_pincode,order_date,consolidated_categories,multi_category_flag, 
delivery_state, delivery_state_code,delivery_district,seller_state,seller_state_code,seller_district) 
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)