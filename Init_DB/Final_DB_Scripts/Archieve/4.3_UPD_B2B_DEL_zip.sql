UPDATE POSTGRES_SCHEMA.AGG_TBL_B2B
SET delivery_state = pc."Statename",
    delivery_state_code = pc."Statecode",
    delivery_district = pc."Districtname"
FROM POSTGRES_SCHEMA.TBL_PINCODE pc
WHERE AGG_TBL_B2B.delivery_pincode = pc."Pincode"
AND AGG_TBL_B2B.delivery_pincode <> ''
AND pc."Statename" IS NOT NULL
AND pc."Districtname" IS NOT NULL
AND AGG_TBL_B2B.order_date = 'DATE_PARAM';
-- AND extract(month from ({AGG_TBL_B2B}.order_date)) = {MNTH};