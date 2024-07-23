INSERT INTO POSTGRES_SCHEMA.SELLER_TBL
(provider_key, order_date, category, sub_category, seller_state_code, seller_state, 
seller_district)
select distinct(trim(provider_key)) as provider_key, order_date,
   consolidated_categories as category, sub_category, seller_state_code,
   seller_state , seller_district
FROM POSTGRES_SCHEMA.AGG_TBL_B2C
   group by 1,2,3,4,5,6,7
