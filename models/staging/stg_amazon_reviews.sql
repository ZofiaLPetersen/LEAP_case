{{ config(materialized='view') }}

with source_data as (

    select *
    from {{ source('raw', 'amazon') }}
    where not (
        upper(trim(product_id)) = 'PRODUCT_ID'
        and upper(trim(product_name)) = 'PRODUCT_NAME'
    )

),

cleaned as (

    select
        trim(product_id) as product_id,
        trim(product_name) as product_name,
        trim(category) as category,

        try_to_decimal(regexp_replace(discounted_price, '[^0-9.]', ''), 10, 2) as discounted_price,
        try_to_decimal(regexp_replace(actual_price, '[^0-9.]', ''), 10, 2) as actual_price,
        try_to_decimal(regexp_replace(discount_percentage, '[^0-9.]', ''), 5, 2) as discount_percentage,

        try_to_decimal(regexp_replace(rating, '[^0-9.]', ''), 3, 2) as rating,
        try_to_number(regexp_replace(rating_count, '[^0-9]', '')) as rating_count,

        nullif(trim(about_product), '') as about_product,
        nullif(trim(img_link), '') as img_link,
        nullif(trim(product_link), '') as product_link,

        trim(user_id) as user_id,
        nullif(trim(user_name), '') as user_name,

        trim(review_id) as review_id,
        nullif(trim(review_title), '') as review_title,
        nullif(trim(review_content), '') as review_content

    from source_data

),

-- 🔥 NEW: explode comma-separated review_ids
exploded as (

    select
        c.product_id,
        c.product_name,
        c.category,

        c.discounted_price,
        c.actual_price,
        c.discount_percentage,

        c.rating,
        c.rating_count,

        c.about_product,
        c.img_link,
        c.product_link,

        c.user_id,
        c.user_name,

        -- split review_id into individual rows
        nullif(trim(split_ids.value), '') as review_id,

        c.review_title,
        c.review_content

    from cleaned c,
    lateral split_to_table(c.review_id, ',') split_ids

),

final as (

    select
        product_id,
        product_name,
        category,

        discounted_price,
        actual_price,
        round(discounted_price * 0.011, 2) as discounted_price_eur,
        round(actual_price * 0.011, 2) as actual_price_eur,

        discount_percentage,
        rating,
        rating_count,
        about_product,
        img_link,
        product_link,
        user_id,
        user_name,
        review_id,
        review_title,
        review_content,

        actual_price - discounted_price as discount_amount,
        round((actual_price - discounted_price) * 0.011, 2) as discount_amount_eur,

        case
            when actual_price > 0 then round((actual_price - discounted_price) / actual_price * 100, 2)
            else null
        end as calculated_discount_pct,

        length(coalesce(review_content, '')) as review_content_length,
        length(coalesce(review_title, '')) as review_title_length,

        case when review_content is not null then 1 else 0 end as has_review_content,
        case when review_title is not null then 1 else 0 end as has_review_title

    from exploded

)

select *
from final