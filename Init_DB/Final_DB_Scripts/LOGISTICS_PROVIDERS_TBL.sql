INSERT INTO POSTGRES_SCHEMA.LOGISTICS_PROVIDERS_TBL(
    provider_key, order_date, seller_state_code, 
    seller_state,seller_district)
SELECT distinct trim(concat(provider_id, '_', bpp_id)) as provider_key,
       order_date,
       seller_state_code,
       seller_state,
       seller_district
from POSTGRES_SCHEMA.AGG_TBL_LOG lfod
where order_date is not null
group by 
       provider_key,
       order_date,
seller_state_code,
seller_state,
seller_district;


-- SELECT distinct trim(concat(provider_id, '_', bpp_id)) as provider_key,
--        order_date,
--        seller_state_code,
--        seller_state,
--        seller_district
-- from POSTGRES_SCHEMA.AGG_TBL_LOG lfod
-- where order_date <> '' or order_date is null
-- group by 
--        provider_key,
--        order_date,
-- seller_state_code,
-- seller_state,
-- seller_district;