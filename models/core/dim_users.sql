{{ config(materialized='table') }}

select distinct
    user_id,
    user_name
from {{ ref('stg_amazon_reviews') }}
where user_id is not null