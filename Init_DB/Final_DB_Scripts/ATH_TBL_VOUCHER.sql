SELECT odv."network order id" AS network_order_id,
    MAX(CONCAT(lower(trim(odv."seller np name")), '-', lower(trim(odv."provider_id")))) AS provider_key,
    COALESCE(SUM(CAST(odv."qty" AS Decimal)),0) AS total_items,
    MAX(case WHEN TRIM(odv."Domain") = 'ONDC:FIS10' THEN 'Retail_Voucher' ELSE 'Others' END) AS domain,
    MAX(case WHEN odv."order_status" IS NULL OR TRIM(odv."order_status") = '' THEN 'In-progress' ELSE TRIM(odv."order_status") END) AS order_status,
    MAX(case WHEN UPPER(odv."delivery pincode") LIKE '%XXX%' OR UPPER(odv."delivery pincode") LIKE '%*%' THEN null ELSE odv."delivery pincode" END) AS delivery_pincode,
    MAX(DATE(odv."o_created_at_date")) AS order_date,
    NULL as delivery_state,
    NULL as delivery_state_code, 
    NULL as delivery_district
FROM ATH_DB.ATH_TBL_VOUCHER odv
WHERE odv."seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
and (DATE(odv."o_created_at_date")) = DATE('{date_val}')
GROUP BY odv."network order id";