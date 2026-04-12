with team_performance as (
    -- We are using the pre-calculated intermediate model (DRY Principle)
    select * from {{ ref('int_team_match_performance') }}
),

teams as (
    select * from {{ ref('stg_soccer__team') }}
),

-- Step 1: Aggregate stats per season and league
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
    from team_performance
    group by 1, 2, 3 
),

-- Step 2: Final ranking logic with Window Functions
final_standings as (
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

select * from final_standings
order by season desc, league_id, league_position asc