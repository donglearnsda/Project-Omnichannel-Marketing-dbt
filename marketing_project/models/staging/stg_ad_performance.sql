with source as (
    select * from {{ source('omnichannel_raw', 'ad_performance') }}
),

renamed as (
    select
        cast(performance_id as string) as performance_id,
        cast(campaign_id as string) as campaign_id,
        cast(creator_id as string) as creator_id,
        cast(date as date) as date,
        cast(spend_vnd as numeric) as spend_vnd,
        cast(impressions as int64) as impressions,
        cast(clicks as int64) as clicks,
        cast(video_views as int64) as video_views,
        cast(reach as int64) as reach,
        platform,
        cast(loaded_at as timestamp) as loaded_at
    from source
)

select * from renamed