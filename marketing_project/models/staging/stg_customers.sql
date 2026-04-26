with source as (
    select * from {{ source('omnichannel_raw', 'customers') }}
),

renamed as (
    select
        cast(customer_id as string) as customer_id,
        acquisition_source,
        region,
        age_group,
        gender,
        cast(registered_at as timestamp) as registered_at
    from source
)

select * from renamed