with source as (
    -- Reference the dbt source defined in src_soccer.yml
    select * from SOCCER_DB.RAW.PLAYER
),

renamed as (
    select
        -- Primary and Foreign Keys
        id as player_pk,
        player_api_id as player_id,
        player_fifa_api_id as fifa_player_id,
        
        -- Clean text data
        trim(player_name) as player_name,
        
        -- Cast timestamp to date for accurate age calculations downstream
        cast(birthday as date) as birthday_date,
        
        -- Physical Attributes (kept in original units for Staging)
        height as height_cm,
        weight as weight_lbs

    from source
)

select * from renamed