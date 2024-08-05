
-- ===============================================================================
-- Logistic

select count (distinct concat(order_id, transaction_id, bap_id, bpp_id, cast(order_date as varchar))) as network_order_id, 
extract( month from order_date) as month_val,
extract( year from order_date) as year_val from 
(WITH base_data AS (
    SELECT
        a.bap_id,
        a.bpp_id,
        a.provider_id,
        a.order_id,
        a.transaction_id,
        a.item_id,
        a.fulfillment_status,
        date(a.order_created_at) AS order_date,
        a.domain,
        date_parse(a.f_agent_assigned_at_date, '%Y-%m-%dT%H:%i:%s') AS f_agent_assigned_at_date,
        CASE
            WHEN UPPER(a.latest_order_status) = 'COMPLETED' THEN 'Delivered'
            WHEN UPPER(a.latest_order_status) = 'CANCELLED' THEN 'Cancelled'
            ELSE 'In Process'
        END AS Log_Ondc_Status,
        a.network_retail_order_id,
        CASE
            WHEN a.bpp_id = 'ondc-lsp.olacabs.com' THEN 'P2P'
            ELSE a.shipment_type
        END AS shipment_type,
        CASE
            WHEN REGEXP_LIKE(a.pick_up_pincode, '^[0-9]+$') THEN CAST(CAST(a.pick_up_pincode AS DOUBLE) AS DOUBLE)
            ELSE -1
        END AS pick_up_pincode,
        CASE
            WHEN REGEXP_LIKE(a.delivery_pincode, '^[0-9]+$') THEN CAST(CAST(a.delivery_pincode AS DOUBLE) AS DOUBLE)
            ELSE -1
        END AS delivery_pincode,
        CASE
            WHEN a.network_retail_order_category IS NULL THEN 'Undefined'
            WHEN a.network_retail_order_category = '' THEN 'Undefined'
            ELSE a.network_retail_order_category
        END AS network_retail_order_category,
        a.on_confirm_sync_response,
        a.on_confirm_error_code,
        a.on_confirm_error_message,
        null as delivery_district,
		null as delivery_state,
		null as delivery_state_code,
        null as seller_state,
        null as seller_district,
        null as seller_state_code
    FROM "default"."shared_open_data_logistics_order" a
    where
    not (lower(bpp_id) like '%test%')
    and not(lower(bap_id) like '%test%')
    and not(lower(bpp_id) like '%preprod%')
    and not(lower(bap_id) like '%demoproject%')
    and not(lower(bpp_id) like '%preprod')
    and DATE(order_created_at) is not null
    -- and DATE(a.order_created_at) = date('{date_val}')
    and a.bap_id is not null
    AND (a.on_confirm_error_code IS NULL OR a.on_confirm_error_code NOT IN ('65001', '66001'))
      AND (a.on_confirm_sync_response <> 'NACK' OR a.on_confirm_sync_response IS NULL)
  AND (a.on_confirm_error_code IS NULL OR a.on_confirm_error_code NOT IN ('65001', '66001'))
),
filtered_data AS (
    SELECT
        a.bap_id,
        a.bpp_id,
        a.provider_id,
        a.order_id,
        a.transaction_id,
        a.item_id,
        a.fulfillment_status,
        date(a.order_created_at) AS order_date,
        a.domain,
        date_parse(a.f_agent_assigned_at_date, '%Y-%m-%dT%H:%i:%s') AS f_agent_assigned_at_date,
        CASE
            WHEN UPPER(a.latest_order_status) = 'COMPLETED' THEN 'Delivered'
            WHEN UPPER(a.latest_order_status) = 'CANCELLED' THEN 'Cancelled'
            ELSE 'In Process'
        END AS Log_Ondc_Status,
        a.network_retail_order_id,
        CASE
            WHEN a.bpp_id = 'ondc-lsp.olacabs.com' THEN 'P2P'
            ELSE a.shipment_type
        END AS shipment_type,
        CASE
            WHEN REGEXP_LIKE(a.pick_up_pincode, '^[0-9]+$') THEN CAST(CAST(a.pick_up_pincode AS DOUBLE) AS DOUBLE)
            ELSE -1
        END AS pick_up_pincode,
        CASE
            WHEN REGEXP_LIKE(a.delivery_pincode, '^[0-9]+$') THEN CAST(CAST(a.delivery_pincode AS DOUBLE) AS DOUBLE)
            ELSE -1
        END AS delivery_pincode,
        CASE
            WHEN a.network_retail_order_category IS NULL THEN 'Undefined'
            WHEN a.network_retail_order_category = '' THEN 'Undefined'
            ELSE a.network_retail_order_category
        END AS network_retail_order_category,
        a.on_confirm_sync_response,
        a.on_confirm_error_code,
        a.on_confirm_error_message,
        null as delivery_district,
		null as delivery_state,
		null as delivery_state_code,
        null as seller_state,
        null as seller_district,
        null as seller_state_code
    FROM "default"."shared_open_data_logistics_order" a
    WHERE date(a.order_created_at) >= DATE('2024-05-01')
    -- and date(a.order_created_at) = date('{date_val}')
        AND date_parse(a.f_agent_assigned_at_date, '%Y-%m-%dT%H:%i:%s') IS NULL
        AND UPPER(a.latest_order_status) = 'CANCELLED'
        AND (CASE
                WHEN a.bpp_id = 'ondc-lsp.olacabs.com' THEN 'P2P'
                ELSE a.shipment_type
            END) = 'P2P'
    and not (lower(bpp_id) like '%test%')
    and not(lower(bap_id) like '%test%')
    and not(lower(bpp_id) like '%preprod%')
    and not(lower(bap_id) like '%demoproject%')
    and not(lower(bpp_id) like '%preprod')
    and a.bap_id is not null
    and DATE(order_created_at) is not null
      AND (a.on_confirm_sync_response <> 'NACK' OR a.on_confirm_sync_response IS NULL)
  AND (a.on_confirm_error_code IS NULL OR a.on_confirm_error_code NOT IN ('65001', '66001'))
)SELECT * FROM base_data
except SELECT * FROM filtered_data)
group by extract( month from order_date), extract( year from order_date)
order by  extract( year from order_date) desc,  extract( month from order_date) desc;

-- ===============================================================================
-- B2B

select count (distinct network_order_id), 
extract(month from order_date) as month_val,
extract(year from order_date) as year_val from (
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
FROM "default"."shared_open_data_b2b_order" odv
WHERE odv."seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
--and date(date_parse("O_Created Date & Time",'%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
GROUP BY odv."network order id")
group by extract(month from order_date), extract(year from order_date)
order by extract(year from order_date) desc, extract(month from order_date) desc;

-- ===============================================================================
-- B2C

select count(network_order_id) from (
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
            "default"."nhm_order_fulfillment_subset_v1"
         WHERE
--            DATE(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
            "seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
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
        "default"."nhm_order_fulfillment_subset_v1"
    WHERE
--        DATE(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
        "seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
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
    "default"."nhm_order_fulfillment_subset_v1" odv
LEFT JOIN order_subcategories oc ON odv."network order id" = oc."network order id" AND odv."Item Category" = oc.sub_category
LEFT JOIN distinct_subcategories dc ON odv."network order id" = dc."network order id" AND odv."Item Category" = dc.sub_category
WHERE
    odv."seller np name" NOT IN ('gl-6912-httpapi.glstaging.in/gl/ondc')
--    AND DATE(date_parse("O_Created Date & Time", '%Y-%m-%dT%H:%i:%s')) = DATE('{date_val}')
GROUP BY
    odv."network order id", odv."Item Category");

