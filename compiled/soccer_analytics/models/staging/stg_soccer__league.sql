with source as (
    -- Reference the seed file from the seeds/ directory
    select * from SOCCER_DB.ci_pr_test.soccer__league
),

renamed as (
    select
        -- Cast IDs to numbers and trim strings to prevent join issues downstream
        cast(id as number) as league_id,
        cast(country_id as number) as country_id,
        trim(name) as league_name
    from source
)

select * from renamed