{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_dates') }}
),

final as (
    select 
        date_day,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(quarter from date_day) as quarter,
        -- Xác định ngày cuối tuần
        case 
            when extract(dayofweek from date_day) in (1, 7) then true 
            else false 
        end as is_weekend
    from staging
)

select * from final