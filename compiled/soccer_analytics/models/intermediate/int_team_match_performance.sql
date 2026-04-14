with matches as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__matches
),

home_games as (
    select
        match_id,
        league_id, 
        season,
        match_date,
        home_team_id as team_id,
        away_team_id as opponent_id,
        'Home' as venue,
        home_team_goal as goals_scored,
        away_team_goal as goals_conceded
    from matches
),

away_games as (
    select
        match_id,
        league_id, 
        season,
        match_date,
        away_team_id as team_id,
        home_team_id as opponent_id,
        'Away' as venue,
        away_team_goal as goals_scored,
        home_team_goal as goals_conceded
    from matches
),

unpivoted_matches as (
    -- Step 1: Combine the data first (Unpivot)
    select * from home_games
    union all
    select * from away_games
),

final_performance as (
    -- Step 2: Calculate business logic (points) ONCE for the unified dataset
    select 
        *,
        case
            when goals_scored > goals_conceded then 3
            when goals_scored = goals_conceded then 1
            else 0
        end as points_earned
    from unpivoted_matches
)

select * from final_performance