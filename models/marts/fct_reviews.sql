select
    review_id,
    product_id,
    user_id,
    review_title,
    review_content,
    review_title_length,
    review_content_length,
    has_review_title,
    has_review_content
from {{ ref('stg_amazon_reviews') }}
where review_id is not null