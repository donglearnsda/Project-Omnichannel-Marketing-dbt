with source as (
    select * from {{ source('omnichannel_raw', 'creator_reviews') }}
),

renamed as (
    select
        cast(review_id as string) as review_id,
        cast(creator_id as string) as creator_id,
        cast(campaign_id as string) as campaign_id,
        reviewer_role,
        cast(rating as int64) as rating,
        sentiment,
        comment,
        cast(reviewed_at as timestamp) as reviewed_at
    from source
)

select * from renamed