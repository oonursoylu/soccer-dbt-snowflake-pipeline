with source as (
    select * from SOCCER_DB.RAW.TEAM
),

renamed as (
    select
        id as team_pk,
        team_api_id as team_id,
        team_fifa_api_id,
        team_long_name,
        team_short_name
    from source
)

select * from renamed