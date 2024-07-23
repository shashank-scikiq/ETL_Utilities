UPDATE POSTGRES_SCHEMA.AGG_TBL_B2C
SET delivery_state = pc."Statename",
    delivery_state_code = pc."Statecode",
    delivery_district = pc."Districtname"
FROM POSTGRES_SCHEMA.TBL_PINCODE pc
WHERE AGG_TBL_B2C.delivery_pincode = pc."Pincode"
AND AGG_TBL_B2C.delivery_pincode <> ''
AND pc."Statename" IS NOT NULL
AND pc."Districtname" IS NOT NULL
AND AGG_TBL_B2C.order_date = 'DATE_PARAM';
-- AND extract(month from ({AGG_TBL_B2C}.order_date)) = {MNTH};