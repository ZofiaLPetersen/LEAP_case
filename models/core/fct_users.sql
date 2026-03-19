{{ config(materialized='table') }}

select
    user_id,
    any_value(user_name) as user_name
from {{ ref('stg_amazon_reviews') }}
where user_id is not null
  and user_id != ''
group by user_id