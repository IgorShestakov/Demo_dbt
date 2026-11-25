{{ config(materialized='table') }}
select
    COUNT(DISTINCT order_id) as order_amount, 
    toDate(order_purchase_timestamp) as order_date,
    order_status,
    now() as update_date
from {{ source('s01_extract_data', 'orders') }}
WHERE 
    order_status <> 'unavailable'
GROUP BY 
    toDate(order_purchase_timestamp), 
    order_status