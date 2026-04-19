{{ config(
    materialized='incremental',
    unique_key='performance_id',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["campaign_id", "creator_id"],
    tags=['fact']
) }}

with staging as (
    select * from {{ ref('stg_ad_performance') }}
)

select * from staging

-- Logic Optimizer: Nếu bảng ĐÃ TỒN TẠI, chỉ quét và cập nhật dữ liệu của 3 ngày gần nhất
-- (Đề phòng trường hợp các Ad Network trả dữ liệu trễ - late arriving data)
{% if is_incremental() %}
  where date >= date_sub(current_date('Asia/Ho_Chi_Minh'), interval 3 day)
{% endif %}