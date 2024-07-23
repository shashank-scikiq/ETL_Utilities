
INSERT INTO POSTGRES_SCHEMA.LOGISTICS_MONTHLY_PROVIDERS_TBL(
    provider_key, seller_district, seller_state, seller_state_code,
    order_month, order_year)
select distinct provider_key as provider_key,
seller_district,
seller_state,
seller_state_code,
date_part('month', order_date::date) as order_month,
date_part('year', order_date::date) as order_year
from POSTGRES_SCHEMA.LOGISTICS_PROVIDERS_TBL dp 
group by provider_key, seller_district,
seller_state,
seller_state_code,
order_month,
order_year