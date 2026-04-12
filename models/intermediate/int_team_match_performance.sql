with matches as (
    select * from {{ ref('stg_soccer__matches') }}
),

home_games as (
    select
        match_id,
        season,
        match_date,
        home_team_id as team_id,
        away_team_id as opponent_id,
        'Home' as venue,
        home_team_goal as goals_scored,
        away_team_goal as goals_conceded,
        
        -- I am calculating match points based on the result for the home team.
        case
            when home_team_goal > away_team_goal then 3
            when home_team_goal = away_team_goal then 1
            else 0
        end as points_earned
    from matches
),

away_games as (
    select
        match_id,
        season,
        match_date,
        away_team_id as team_id,
        home_team_id as opponent_id,
        'Away' as venue,
        away_team_goal as goals_scored,
        home_team_goal as goals_conceded,
        
        -- I am calculating match points based on the result for the away team.
        case
            when away_team_goal > home_team_goal then 3
            when away_team_goal = home_team_goal then 1
            else 0
        end as points_earned
    from matches
),

unpivoted_matches as (
    -- I am combining home and away games to create a unified performance view per team per match.
    select * from home_games
    union all
    select * from away_games
)

select * from unpivoted_matches