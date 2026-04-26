with source as (
    select * from {{ source('omnichannel_raw', 'campaigns') }}
),

renamed as (
    select
        cast(campaign_id as string) as campaign_id,
        campaign_name,
        source_platform,
        marketing_objective,
        budget_type,
        cast(total_budget_vnd as numeric) as total_budget_vnd,
        target_audience,
        product_category,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        cast(created_at as timestamp) as created_at
    from source
)

select * from renamed