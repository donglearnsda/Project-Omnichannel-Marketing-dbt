with staging as (
    -- Chú ý: Ở lớp này dùng hàm ref() để gọi bảng Staging thay vì source()
    select * from {{ ref('stg_creators') }}
),

calculated_scd2 as (
    select
        -- 1. Tạo Surrogate Key: Khóa chính đại diện cho mỗi phiên bản của creator
        to_hex(md5(concat(creator_id, '|', cast(updated_at as string)))) as surrogate_key,
        
        creator_id,
        name,
        platform,
        follower_count,
        category,
        engagement_rate,
        tier,
        status,
        country,
        
        -- 2. Thời điểm bắt đầu có hiệu lực (chính là updated_at của dòng hiện tại)
        updated_at as valid_from,
        
        -- 3. Cốt lõi của SCD2: Dùng Window Function LEAD() để "nhìn" xuống dòng tiếp theo
        -- Thời điểm hết hiệu lực = thời điểm bản ghi tiếp theo xuất hiện
        lead(updated_at) over (
            partition by creator_id 
            order by updated_at asc
        ) as valid_to

    from staging
),

final as (
    select
        *,
        -- 4. Xác định phiên bản hiện tại (Current Version)
        -- Nếu valid_to là NULL (không có dòng nào nối tiếp), thì đây là bản mới nhất
        case 
            when valid_to is null then true 
            else false 
        end as is_current_version
    from calculated_scd2
)

select * from final