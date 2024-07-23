WITH order_subcategories AS (
    SELECT
        "network order id",
        "Item Category" AS sub_category,
        array_join(CAST(array_agg(COALESCE("Item Consolidated Category", 'Missing')) AS array(varchar)), ',') AS consolidated_categories,
        COUNT(DISTINCT "Item Consolidated Category") AS category_count
    FROM
        (SELECT
            "network order id", "Item Consolidated Category", "Item Category"
         FROM
            ATH_DB.ATH_TBL_B2C
         WHERE
            DATE(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
            AND "seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
         GROUP BY
            "network order id", "Item Consolidated Category", "Item Category"
        )
    GROUP BY
        "network order id", "Item Category"
),
distinct_subcategories AS (
    SELECT
        "network order id",
        "Item Category" AS sub_category
    FROM
        ATH_DB.ATH_TBL_B2C
    WHERE
        DATE(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
        AND "seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
    GROUP BY
        "network order id", "Item Category"
)
SELECT
    odv."network order id" AS network_order_id,
    odv."Item Category" AS sub_category,
    MAX(odv."seller np name") AS seller_np,
    COALESCE(SUM(CAST(odv."Qty" AS Decimal)), 0) AS total_items,
    MAX(CASE WHEN TRIM(odv."Domain") = 'nic2004:52110' THEN 'Retail' ELSE 'Others' END) AS domain,
    MAX(CONCAT(lower(trim(odv."seller np name")), '-', lower(trim(odv."Provider id")))) AS provider_key,
    MAX(CASE WHEN odv."Order Status" IS NULL OR TRIM(odv."Order Status") = '' THEN 'In-progress' ELSE TRIM(odv."Order Status") END) AS order_status,
    MAX(CASE WHEN UPPER(odv."Seller Pincode") LIKE '%XXX%' OR UPPER(odv."Seller Pincode") LIKE '%*%' THEN NULL ELSE odv."Seller Pincode" END) AS seller_pincode,
    MAX(CASE WHEN UPPER(odv."Delivery Pincode") LIKE '%XXX%' OR UPPER(odv."Delivery Pincode") LIKE '%*%' THEN NULL ELSE odv."Delivery Pincode" END) AS delivery_pincode,
    MAX(DATE(SUBSTRING(odv."O_Created Date & Time", 1, 10))) AS order_date,
    MAX(oc.consolidated_categories) AS consolidated_categories,
    MAX(CASE WHEN oc.category_count > 1 THEN 1 ELSE 0 END) AS multi_category_flag,
    NULL AS delivery_state,
    NULL AS delivery_state_code,
    NULL AS delivery_district,
    NULL AS seller_state,
    NULL AS seller_state_code,
    NULL AS seller_district
FROM
    ATH_DB.ATH_TBL_B2C odv
LEFT JOIN order_subcategories oc ON odv."network order id" = oc."network order id" AND odv."Item Category" = oc.sub_category
LEFT JOIN distinct_subcategories dc ON odv."network order id" = dc."network order id" AND odv."Item Category" = dc.sub_category
WHERE
    odv."seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
    AND DATE(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
GROUP BY
    odv."network order id", odv."Item Category";
