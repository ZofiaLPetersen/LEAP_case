{{ config(materialized='table') }}

select distinct
    product_id,
    product_name,
    category,
    discounted_price_eur,
    actual_price_eur,
    discount_percentage,
    rating,
    rating_count,
    about_product,
    img_link,
    product_link,
    discount_amount,
    calculated_discount_pct
from {{ ref('stg_amazon_reviews') }}
where product_id is not null