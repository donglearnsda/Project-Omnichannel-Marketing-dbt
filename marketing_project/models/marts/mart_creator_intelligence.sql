{{ config(materialized='table', tags=['mart', 'reporting']) }}

with creators as (
    -- Chỉ lấy phiên bản hiện tại của Creator (SCD2 logic phát huy tác dụng ở đây)
    select * from {{ ref('dim_creators') }} 
    where is_current = true
),

ad_spend as (
    -- Tổng hợp chi phí theo từng Creator
    select 
        creator_id, 
        sum(spend_vnd) as total_spend 
    from {{ ref('fct_ad_performance') }}
    group by 1
),

conversions as (
    -- Tổng hợp doanh thu theo từng Creator
    select 
        creator_id, 
        sum(order_amount_vnd) as total_revenue 
    from {{ ref('fct_conversions') }}
    group by 1
),

final as (
    select
        c.creator_id,
        c.name as creator_name,
        coalesce(s.total_spend, 0) as total_spend,
        coalesce(r.total_revenue, 0) as total_revenue,
        
        -- Tính Return on Investment (ROI): (Doanh thu - Chi phí) / Chi phí
        -- Dùng safe_divide của BigQuery để tránh lỗi chia cho 0
        safe_divide(
            (coalesce(r.total_revenue, 0) - coalesce(s.total_spend, 0)), 
            coalesce(s.total_spend, 0)
        ) as avg_roi,
        
        -- Tính Return on Ad Spend (ROAS): Doanh thu / Chi phí
        safe_divide(
            coalesce(r.total_revenue, 0), 
            coalesce(s.total_spend, 0)
        ) as roas

    from creators c
    left join ad_spend s on c.creator_id = s.creator_id
    left join conversions r on c.creator_id = r.creator_id
)

select * from final