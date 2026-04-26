with source as (
    select * from {{ source('omnichannel_raw', 'conversions') }}
),

renamed as (
    select
        cast(order_id as string) as order_id,
        cast(customer_id as string) as customer_id,
        cast(campaign_id as string) as campaign_id,
        cast(creator_id as string) as creator_id,
        cast(order_amount_vnd as numeric) as order_amount_vnd,
        currency,
        payment_method,
        order_status,
        platform,
        cast(timestamp as timestamp) as order_timestamp,
        -- Extract date từ timestamp để làm khóa join và partition sau này
        date(cast(timestamp as timestamp)) as order_date, 
        cast(loaded_at as timestamp) as loaded_at
    from source
)

select * from renamed