INSERT INTO POSTGRES_SCHEMA.AGG_TBL_LOG
(bap_id, bpp_id, provider_id, order_id, transaction_id, item_id, fulfillment_status, order_date, "domain",
 f_agent_assigned_at_date, "Log_Ondc_Status", network_retail_order_id, shipment_type, pick_up_pincode, 
 delivery_pincode, network_retail_order_category, on_confirm_sync_response, on_confirm_error_code, 
 on_confirm_error_message, delivery_district, delivery_state, delivery_state_code, seller_state, seller_district, 
 seller_state_code)
VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)