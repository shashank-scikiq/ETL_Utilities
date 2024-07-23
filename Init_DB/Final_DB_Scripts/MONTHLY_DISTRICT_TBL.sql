INSERT INTO POSTGRES_SCHEMA.MONTHLY_DISTRICT_TBL
(domain_name, sub_domain, order_month, order_year, delivery_district, 
delivery_state, delivery_state_code, seller_district, seller_state, 
seller_state_code, total_orders_delivered, intrastate_orders, intradistrict_orders)
select
        'Retail' as domain_name,
        'Voucher' as sub_domain,
        date_part('month', order_date) as order_month,
        date_part('year', order_date) as order_year,
        delivery_district,
        delivery_state,
        delivery_state_code,
        'Missing' as seller_district,
        'Missing' as seller_state,
        'Missing' as seller_state_code,
        COALESCE(sum(total_orders_delivered),0) as total_orders_delivered,
        0 as intrastate_orders,
        0 as intradistrict_orders
from POSTGRES_SCHEMA.RV_DISTRICT_TBL 
group by date_part('month', order_date), date_part('year', order_date),
delivery_district,
        delivery_state,
        delivery_state_code,
        seller_district,
        seller_state,
        seller_state_code
UNION
    select         
        'Retail' as domain_name,
        'B2C' as sub_domain,
        date_part('month', order_date) as order_month,
        date_part('year', order_date) as order_year,
        delivery_district,
        delivery_state,
        delivery_state_code,
        seller_district,
        seller_state,
        seller_state_code,
        COALESCE(sum(total_orders_delivered),0) as total_orders_delivered,
        COALESCE(sum(intrastate_orders),0) as intrastate_orders,
        COALESCE(sum(intradistrict_orders),0) as intradistrict_orders
from POSTGRES_SCHEMA.DISTRICT_TBL 
group by date_part('month', order_date), date_part('year', order_date),
delivery_district,
        delivery_state,
        delivery_state_code,seller_district,
        seller_state,
        seller_state_code
union 
select         
        'Retail' as domain_name,
        'B2B' as sub_domain,
        date_part('month', order_date) as order_month,
        date_part('year', order_date) as order_year,
        delivery_district,
        delivery_state,
        delivery_state_code,
        'Missing' as seller_district,
        'Missing' as seller_state,
        'Missing' as seller_state_code,
        COALESCE(sum(total_orders_delivered),0) as total_orders_delivered,
        0 as intrastate_orders,
        0 as intradistrict_orders
from POSTGRES_SCHEMA.B2B_DISTRICT_TBL
group by date_part('month', order_date), date_part('year', order_date),
delivery_district,
        delivery_state,
        delivery_state_code,
        seller_district,
        seller_state,
        seller_state_code
    UNION
    select         
        'Logistics' as domain_name,
        'Logistics_Detail' as sub_domain,
        date_part('month', order_date) as order_month,
        date_part('year', order_date) as order_year,
        delivery_district,
        delivery_state,
        delivery_state_code,
        seller_district,
        seller_state,
        seller_state_code,
        COALESCE(sum(total_orders_delivered),0) as total_orders_delivered,
        COALESCE(sum(intrastate_orders),0) as intrastate_orders,
        COALESCE(sum(intradistrict_orders),0) as intradistrict_orders
from POSTGRES_SCHEMA.LOGISTICS_DISTRICT_TBL
group by date_part('month', order_date), date_part('year', order_date),
delivery_district,
        delivery_state,
        delivery_state_code,
        seller_district,
        seller_state,
        seller_state_code;