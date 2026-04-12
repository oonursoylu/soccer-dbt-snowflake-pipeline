/* This model calculates the professional league standings.
    It includes:
    - Points and Goal stats
    - W/D/L (Wins, Draws, Losses) breakdown
    - Dynamic league position (Rank) based on European tie-breaking rules
*/

with matches as (
    select * from {{ ref('stg_soccer__matches') }}
),

teams as (
    select * from {{ ref('stg_soccer__team') }}
),

-- Extracting and calculating results for home teams with inline points logic
home_results as (
    select
        season,
        league_id,
        home_team_id as team_id,
        home_team_goal as goals_scored,
        away_team_goal as goals_conceded,
        case 
            when home_team_goal > away_team_goal then 3
            when home_team_goal = away_team_goal then 1
            else 0
        end as points_earned
    from matches
),

-- Extracting and calculating results for away teams with inline points logic
away_results as (
    select
        season,
        league_id,
        away_team_id as team_id,
        away_team_goal as goals_scored,
        home_team_goal as goals_conceded,
        case 
            when away_team_goal > home_team_goal then 3
            when away_team_goal = home_team_goal then 1
            else 0
        end as points_earned
    from matches
),

-- Combining all performances
combined_results as (
    select * from home_results
    union all
    select * from away_results
),

-- Calculating aggregate stats including Win/Draw/Loss counts
season_stats as (
    select
        season,
        league_id,
        team_id,
        count(*) as matches_played,
        sum(case when points_earned = 3 then 1 else 0 end) as wins,
        sum(case when points_earned = 1 then 1 else 0 end) as draws,
        sum(case when points_earned = 0 then 1 else 0 end) as losses,
        sum(goals_scored) as total_goals_scored,
        sum(goals_conceded) as total_goals_conceded,
        sum(goals_scored) - sum(goals_conceded) as goal_difference,
        sum(points_earned) as total_points
    from combined_results
    group by 1, 2, 3
),

-- Adding Team names and calculating League Positions using Window Functions
final as (
    select
        rank() over (
            partition by s.season, s.league_id 
            order by s.total_points desc, s.goal_difference desc, s.total_goals_scored desc
        ) as league_position,
        s.season,
        s.league_id,
        t.team_long_name as team_name,
        s.matches_played,
        s.wins,
        s.draws,
        s.losses,
        s.total_points,
        s.total_goals_scored,
        s.total_goals_conceded,
        s.goal_difference
    from season_stats s
    left join teams t on s.team_id = t.team_id
)

select * from final
order by season desc, league_id, league_position asc