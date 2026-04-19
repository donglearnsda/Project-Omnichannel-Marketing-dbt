{{ config(
    materialized='table',
    tags=['dimension']
) }}

with intermediate as (
    select * from {{ ref('int_creators') }}
),

final as (
    select
        surrogate_key,
        creator_id,
        name,
        platform,
        follower_count,
        category,
        engagement_rate,
        tier,
        status,
        country,
        -- Đổi tên cột cho khớp chính xác với file schema.yml đã thiết kế
        is_current_version as is_current, 
        valid_from,
        valid_to
    from intermediate
)

select * from final