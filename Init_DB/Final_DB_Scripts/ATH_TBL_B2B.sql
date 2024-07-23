SELECT odv."network order id" AS network_order_id,
    MAX(odv."seller np name") AS seller_np,
    COALESCE(SUM(CAST(odv."qty" AS Decimal)),0) AS total_items,MAX(case WHEN TRIM(odv."Domain") = 'ONDC:RET10' THEN 'B2B' ELSE 'Others' END) AS domain,
    MAX(CONCAT(lower(trim(odv."seller np name")), '-', lower(trim(odv."provider_id")))) AS provider_key,
    MAX(case WHEN odv."order status" IS NULL OR TRIM(odv."order status") = '' THEN 'In-progress' ELSE TRIM(odv."order status") END) AS order_status,
    MAX(case WHEN UPPER(odv."Delivery Pincode") LIKE '%XXX%' OR UPPER(odv."Delivery Pincode") LIKE '%*%' THEN null ELSE odv."Delivery Pincode" END) AS delivery_pincode,
    MAX(DATE(SUBSTRING(odv."O_Created Date & Time", 1, 10))) AS order_date,
   NULL as delivery_state,
   NULL as delivery_state_code, 
   NULL as delivery_district
FROM ATH_DB.ATH_TBL_B2B odv
WHERE odv."seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
and date(date_parse("O_Created Date & Time",'%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
GROUP BY odv."network order id";