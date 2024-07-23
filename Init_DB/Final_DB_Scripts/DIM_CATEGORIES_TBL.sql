INSERT INTO POSTGRES_SCHEMA.DIM_CATEGORIES_TBL
(category, sub_category)
select distinct consolidated_categories  as category, sub_category
from POSTGRES_SCHEMA.AGG_TBL_B2C
group by 1,2