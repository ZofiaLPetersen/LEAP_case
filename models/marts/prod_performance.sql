{{ config(materialized='table') }}

select
    p.product_id,
    p.product_name,
    p.category,
    p.discounted_price_eur,
    p.actual_price_eur,
    p.discount_percentage,
    p.calculated_discount_pct,
    p.discount_amount_eur,
    p.rating,
    p.rating_count,
    count(r.review_id) as review_count,
    count(distinct r.user_id) as unique_reviewers,
    avg(r.review_content_length) as avg_review_length,
    avg(r.review_title_length) as avg_review_title_length,
    sum(r.has_review_content) as reviews_with_content,
    sum(r.has_review_title) as reviews_with_title
from {{ ref('dim_products') }} p
left join {{ ref('fct_reviews') }} r
    on p.product_id = r.product_id
group by
    p.product_id,
    p.product_name,
    p.category,
    p.discounted_price_eur,
    p.actual_price_eur,
    p.discount_percentage,
    p.calculated_discount_pct,
    p.discount_amount_eur,
    p.rating,
    p.rating_count