{{ config(
    materialized='incremental',
    unique_key='order_id',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["campaign_id", "creator_id"],
    tags=['fact']
) }}

with staging as (
    select * from {{ ref('stg_conversions') }}
)

select * from staging

{% if is_incremental() %}
  where order_date >= date_sub(current_date('Asia/Ho_Chi_Minh'), interval 3 day)
{% endif %}