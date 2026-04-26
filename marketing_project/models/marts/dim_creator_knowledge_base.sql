{{ config(materialized='table', tags=['mart', 'ai_rag']) }}

with creators as (
    select * from {{ ref('dim_creators') }} where is_current = true
),

intelligence as (
    select * from {{ ref('mart_creator_intelligence') }}
),

reviews as (
    -- Gom tất cả comment đánh giá của một creator thành một cục text dài
    select 
        creator_id, 
        string_agg(comment, ' | ') as all_reviews 
    from {{ ref('stg_creator_reviews') }}
    group by 1
),

final as (
    select
        c.creator_id,
        -- Dùng hàm CONCAT để nối các mảnh thông tin thành 1 đoạn văn (Text Blob)
        concat(
            'Nhà sáng tạo nội dung ', c.name, ' hoạt động chủ yếu trên nền tảng ', c.platform, '. ',
            'Hiện đang thuộc hạng ', c.tier, ' với tổng cộng ', cast(c.follower_count as string), ' người theo dõi. ',
            'Chuyên môn chính là: ', coalesce(c.category, 'Tổng hợp'), '. ',
            'Về mặt hiệu suất kinh doanh, ROI trung bình mang lại là: ', cast(round(coalesce(i.avg_roi, 0) * 100, 2) as string), '%. ',
            'Đánh giá từ đội ngũ nội bộ: ', coalesce(r.all_reviews, 'Chưa có đánh giá nào.')
        ) as knowledge_text
        
    from creators c
    left join intelligence i on c.creator_id = i.creator_id
    left join reviews r on c.creator_id = r.creator_id
)

select * from final