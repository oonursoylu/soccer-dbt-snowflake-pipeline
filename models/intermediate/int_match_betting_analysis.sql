with matches as (
    select * from {{ ref('stg_soccer__matches') }}
),

betting_logic as (
    select
        match_id,
        match_date,
        home_team_goal,
        away_team_goal,
        odds_home_win,
        odds_draw,
        odds_away_win,

        -- Identify the favorite
        case 
            when odds_home_win < odds_away_win and odds_home_win < odds_draw then 'Home'
            when odds_away_win < odds_home_win and odds_away_win < odds_draw then 'Away'
            else 'Draw' 
        end as bookie_favorite,

        -- Validate the prediction
        case
            when (odds_home_win < odds_away_win and odds_home_win < odds_draw) and (home_team_goal > away_team_goal) then 'Correct'
            when (odds_away_win < odds_home_win and odds_away_win < odds_draw) and (away_team_goal > home_team_goal) then 'Correct'
            when (odds_draw < odds_home_win and odds_draw < odds_away_win) and (home_team_goal = away_team_goal) then 'Correct'
            else 'Wrong'
        end as prediction_status

    from matches
    where odds_home_win is not null -- Filter valid odds only
)

select * from betting_logic