update POSTGRES_SCHEMA.AGG_TBL_B2C
set consolidated_categories = 'Multi Category'
where multi_category_flag = 1
AND AGG_TBL_B2C.order_date = 'DATE_PARAM';
-- AND extract(month from ({AGG_TBL_B2C}.order_date)) = {MNTH};