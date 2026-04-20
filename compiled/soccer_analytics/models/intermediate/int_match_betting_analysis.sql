with matches as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__matches
    where odds_home_win is not null
      and odds_draw     is not null
      and odds_away_win is not null
),

find_favorite as (
    select
        match_id,
        season,
        league_id,
        match_date,
        home_team_id,
        away_team_id,
        home_team_goal,
        away_team_goal,
        odds_home_win,
        odds_draw,
        odds_away_win,

        least(odds_home_win, odds_draw, odds_away_win) as min_odds
    from matches
),

classified_favorite as (
    select
        *,
        case
            when odds_home_win = min_odds
                 and odds_away_win <> min_odds
                 and odds_draw     <> min_odds then 'Home'
            when odds_away_win = min_odds
                 and odds_home_win <> min_odds
                 and odds_draw     <> min_odds then 'Away'
            when odds_draw = min_odds
                 and odds_home_win <> min_odds
                 and odds_away_win <> min_odds then 'Draw'
            else 'Tie'
        end as bookie_favorite
    from find_favorite
),

validate_prediction as (
    select
        *,
        case
            when bookie_favorite = 'Home' and home_team_goal >  away_team_goal then 'Correct'
            when bookie_favorite = 'Away' and away_team_goal >  home_team_goal then 'Correct'
            when bookie_favorite = 'Draw' and home_team_goal =  away_team_goal then 'Correct'
            when bookie_favorite = 'Tie'                                       then 'Ambiguous'
            else 'Wrong'
        end as prediction_status
    from classified_favorite
)

select
    match_id,
    season,
    league_id,
    match_date,
    home_team_id,
    away_team_id,
    home_team_goal,
    away_team_goal,
    odds_home_win,
    odds_draw,
    odds_away_win,
    bookie_favorite,
    prediction_status
from validate_prediction