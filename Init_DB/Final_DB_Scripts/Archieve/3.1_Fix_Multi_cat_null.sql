update POSTGRES_SCHEMA.AGG_TBL_B2C
set consolidated_categories = 'Missing'
where consolidated_categories is null
AND AGG_TBL_B2C.order_date = 'DATE_PARAM';
-- AND extract(month from ({AGG_TBL_B2C}.order_date)) = {MNTH};