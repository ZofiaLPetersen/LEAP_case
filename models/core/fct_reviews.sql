{{ config(materialized="table") }}

with
    deduped as (

        select
            review_id,
            product_id,
            user_id,
            review_title,
            review_content,
            review_title_length,
            review_content_length,
            has_review_title,
            has_review_content,
            row_number() over (
                partition by review_id, product_id, user_id
                order by review_title, review_content
            ) as rn
        from {{ ref("stg_amazon_reviews") }}
        where review_id is not null

    )

select
    
        coalesce(review_id, '')
        || '|'
        || coalesce(product_id, '')
        || '|'
        || coalesce(user_id, '')
    as review_sk,
    review_id,
    product_id,
    user_id,
    review_title,
    review_content,
    review_title_length,
    review_content_length,
    has_review_title,
    has_review_content
from deduped
where rn = 1
