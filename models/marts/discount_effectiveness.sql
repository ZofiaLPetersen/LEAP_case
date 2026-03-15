{{ config(materialized='table') }}

with bucketed as (

    select
        product_id,
        product_name,
        category,
        rating,
        rating_count,
        review_content_length,
        user_id,
        case
            when calculated_discount_pct < 10 then '0-10%'
            when calculated_discount_pct < 20 then '10-20%'
            when calculated_discount_pct < 30 then '20-30%'
            when calculated_discount_pct < 40 then '30-40%'
            when calculated_discount_pct < 50 then '40-50%'
            else '50%+'
        end as discount_band
    from {{ ref('stg_amazon_reviews') }}
    where calculated_discount_pct is not null

)

select
    discount_band,
    count(distinct product_id) as product_count,
    count(distinct user_id) as unique_reviewers,
    avg(rating) as avg_rating,
    avg(rating_count) as avg_rating_count,
    avg(review_content_length) as avg_review_length
from bucketed
group by discount_band
order by
    case discount_band
        when '0-10%' then 1
        when '10-20%' then 2
        when '20-30%' then 3
        when '30-40%' then 4
        when '40-50%' then 5
        else 6
    end