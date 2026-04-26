with source as (
    select * from {{ source('omnichannel_raw', 'dates') }}
),

renamed as (
    select
        cast(date_day as date) as date_day
    from source
)

select * from renamed