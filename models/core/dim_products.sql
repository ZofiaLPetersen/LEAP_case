{{ config(materialized='table') }}

with ranked as (

select
    product_id,
    product_name,
    category,
    discounted_price,
    actual_price,
    discounted_price_eur,
    actual_price_eur,
    discount_percentage,
    rating,
    rating_count,
    about_product,
    img_link,
    product_link,
    discount_amount,
    discount_amount_eur,
    calculated_discount_pct,
    row_number() over (
        partition by product_id
        order by rating_count desc
    ) as rn

from {{ ref('stg_amazon_reviews') }}
where product_id is not null

)

select
    product_id,
    product_name,
    category,
    discounted_price,
    actual_price,
    discounted_price_eur,
    actual_price_eur,
    discount_percentage,
    rating,
    rating_count,
    about_product,
    img_link,
    product_link,
    discount_amount,
    discount_amount_eur,
    calculated_discount_pct
from ranked
where rn = 1