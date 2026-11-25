{{ config(materialized='table') }}
select
    rev.review_score,
    toDate(nullIf(o.order_purchase_timestamp, '')) as order_date,
    count(distinct rev.review_id) as am_review
from {{ source('s01_extract_data', 'order_reviews') }} as rev
left join {{ source('s01_extract_data', 'orders') }} as o
    on rev.order_id = o.order_id
group by
    rev.review_score,
    order_date