WITH base_data AS (
    SELECT
        a.bap_id,
        a.bpp_id,
        a.provider_id,
        a.order_id,
        a.transaction_id,
        a.item_id,
        a.fulfillment_status,
        -- date(a.order_created_at) AS order_date,
        date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) AS order_date,
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
    FROM ATH_DB.ATH_TBL_LOG a
    where
    not (lower(bpp_id) like '%test%')
    and not(lower(bap_id) like '%test%')
    and not(lower(bpp_id) like '%preprod%')
    and not(lower(bap_id) like '%demoproject%')
    and not(lower(bpp_id) like '%preprod')
    -- and DATE(order_created_at) is not null
    and date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) is not null
    -- and DATE(a.order_created_at) = date('{date_val}')
    and date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) = date('{date_val}')
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
        -- date(a.order_created_at) AS order_date,
        date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) AS order_date,
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
    FROM ATH_DB.ATH_TBL_LOG a
    -- WHERE date(a.order_created_at) >= DATE('2024-05-01')
    where date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) >= DATE('2025-05-01')
    -- and date(a.order_created_at) = date('{date_val}')
    and date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) = Date('{date_val}')
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
    -- and DATE(order_created_at) is not null
    and date(date_parse(a.order_created_at, '%Y-%m-%dT%H:%i:%s')) is not null
      AND (a.on_confirm_sync_response <> 'NACK' OR a.on_confirm_sync_response IS NULL)
  AND (a.on_confirm_error_code IS NULL OR a.on_confirm_error_code NOT IN ('65001', '66001'))
)SELECT * FROM base_data
except SELECT * FROM filtered_data;