with source as (
    -- Reference the seed file from the seeds/ directory
    select * from {{ ref('soccer__country') }}
),

renamed as (
    select
        -- Cast IDs to numbers and trim strings to prevent join issues downstream
        cast(id as number) as country_id,
        trim(name) as country_name
    from source
)

select * from renamed