create index if not exists idx_fod on POSTGRES_SCHEMA.AGG_TBL_B2C(sub_category,seller_np, 
"domain", order_date,consolidated_categories,seller_state,seller_district,
delivery_state,delivery_district);