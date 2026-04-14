with source as (
    -- Reference the dbt source defined in src_soccer.yml
    select * from SOCCER_DB.RAW.PLAYER_ATTRIBUTES
),

renamed as (
    select
        -- Primary and Foreign Keys
        id as attribute_pk,
        player_api_id as player_id,
        
        -- Cast timestamp to date for cleaner time-series analysis downstream
        cast(date as date) as rating_date,
        
        -- Core Ratings
        overall_rating,
        potential,
        preferred_foot,
        
        -- Standardize text inputs: trim whitespace and convert to lowercase
        lower(trim(attacking_work_rate)) as attacking_work_rate,
        lower(trim(defensive_work_rate)) as defensive_work_rate,

        -- Physical attributes for performance analysis
        acceleration,
        sprint_speed,
        stamina,
        strength,

        -- Technical skills to distinguish player roles
        finishing,
        short_passing,
        volleys,
        dribbling,
        long_passing,
        ball_control,

        -- Defensive and tactical awareness
        interceptions,
        positioning,
        vision,
        marking,
        standing_tackle

    from source
)

select * from renamed