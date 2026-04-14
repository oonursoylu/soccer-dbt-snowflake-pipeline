with source as (
    -- Reference the dbt source defined in src_soccer.yml
    select * from SOCCER_DB.RAW.MATCHES
),

renamed as (
    select
        -- Primary and Foreign Keys
        id as match_pk,
        match_api_id as match_id,
        country_id,
        league_id,
        
        -- Match Details
        season,
        stage, 
        -- Cast timestamp to date to optimize performance and downstream BI usage
        cast(date as date) as match_date,
        
        -- Team Info
        home_team_api_id as home_team_id,
        away_team_api_id as away_team_id,
        
        -- Match Outcomes
        home_team_goal,
        away_team_goal,
        
        -- Betting Odds (Bet365)
        b365h as odds_home_win,
        b365d as odds_draw,
        b365a as odds_away_win

    from source
)

select * from renamed