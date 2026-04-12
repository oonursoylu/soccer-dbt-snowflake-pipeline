with matches as (
    -- Filtering nulls early is better for performance
    select * from {{ ref('stg_soccer__matches') }}
    where odds_home_win is not null
),

find_favorite as (
    select
        match_id,
        match_date,
        home_team_goal,
        away_team_goal,
        odds_home_win,
        odds_draw,
        odds_away_win,

        -- Step 1: Identify the favorite
        case 
            when odds_home_win < odds_away_win and odds_home_win < odds_draw then 'Home'
            when odds_away_win < odds_home_win and odds_away_win < odds_draw then 'Away'
            else 'Draw' 
        end as bookie_favorite
    from matches
),

validate_prediction as (
    select
        *,
        -- Step 2: Validate the prediction using the calculated column (DRY Principle)
        case
            when bookie_favorite = 'Home' and home_team_goal > away_team_goal then 'Correct'
            when bookie_favorite = 'Away' and away_team_goal > home_team_goal then 'Correct'
            when bookie_favorite = 'Draw' and home_team_goal = away_team_goal then 'Correct'
            else 'Wrong'
        end as prediction_status
    from find_favorite
)

select * from validate_prediction