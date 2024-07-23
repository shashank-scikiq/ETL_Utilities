UPDATE POSTGRES_SCHEMA.AGG_TBL_LOG 
SET delivery_state = pc."Statename",
    delivery_state_code = pc."Statecode",
    delivery_district = pc."Districtname"
FROM POSTGRES_SCHEMA.TBL_PINCODE pc
WHERE AGG_TBL_LOG.delivery_pincode = pc."Pincode" 
AND pc."Pincode"  <> ''
AND pc."Statename" IS NOT NULL
AND pc."Districtname" IS NOT null
AND AGG_TBL_LOG.order_date = 'DATE_PARAM';
-- AND extract(month from ({AGG_TBL_LOG}."Date")) = {MNTH};