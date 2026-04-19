with source as (
    select * from {{ source('omnichannel_raw', 'creators') }}
),

renamed as (
    select
        cast(creator_id as string) as creator_id,
        name,
        platform,
        cast(follower_count as int64) as follower_count,
        category,
        cast(engagement_rate as float64) as engagement_rate,
        tier,
        status,
        country,
        cast(updated_at as timestamp) as updated_at
    from source
)

select * from renamed