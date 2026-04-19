{{ config(materialized='table') }}

with staging as (
    select * from {{ ref('stg_campaigns') }}
)

select * from staging