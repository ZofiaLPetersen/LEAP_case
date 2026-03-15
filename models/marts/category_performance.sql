{{ config(materialized='table') }}

select
    category,
    count(distinct product_id) as product_count,
    count(review_id) as review_count,
    count(distinct user_id) as unique_reviewers,
    avg(rating) as avg_rating,
    avg(discounted_price) as avg_discounted_price,
    avg(actual_price) as avg_actual_price,
    avg(discount_amount) as avg_discount_amount,
    avg(calculated_discount_pct) as avg_discount_pct,
    avg(review_content_length) as avg_review_length
from {{ ref('stg_amazon_reviews') }}
group by category