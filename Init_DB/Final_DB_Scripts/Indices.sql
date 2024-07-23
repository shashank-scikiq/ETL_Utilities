 >>>> create index if not exists idx_fod_pc on POSTGRES_SCHEMA.AGG_TBL_B2C(delivery_pincode,seller_pincode);

 >>>> create index if not exists idx_fod on POSTGRES_SCHEMA.AGG_TBL_B2C(sub_category,seller_np, 
"domain", order_date,consolidated_categories,seller_state,seller_district,
delivery_state,delivery_district);

 >>>> create index if not exists idx_swclo on POSTGRES_SCHEMA.CATEGORY_TBL(order_date,category, delivery_district, delivery_state, 
seller_state, seller_district, seller_state_code);

 >>>> create index if not exists idx_swdlo on POSTGRES_SCHEMA.DISTRICT_TBL(order_date, delivery_district, delivery_state, 
seller_state, seller_district, seller_state_code);

 >>>> create index if not exists idx_swsclo on POSTGRES_SCHEMA.SUB_CATEGORY_TBL(order_date, category, sub_category, delivery_district, 
delivery_state, seller_state, seller_district, seller_state_code, delivery_state_code)