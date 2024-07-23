UPDATE POSTGRES_SCHEMA.AGG_TBL_LOG 
SET seller_state = pc."Statename",
    seller_state_code = pc."Statecode",
    seller_district = pc."Districtname"
FROM POSTGRES_SCHEMA.TBL_PINCODE pc
WHERE AGG_TBL_LOG.pick_up_pincode = pc."Pincode" 
AND pc."Pincode"  <> ''
AND pc."Statename" IS NOT NULL
AND pc."Districtname" IS NOT null
AND AGG_TBL_LOG.order_date = 'DATE_PARAM';
-- AND extract(month from ({AGG_TBL_LOG}."Date")) = {MNTH};