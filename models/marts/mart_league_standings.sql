with team_performance as (
    select * from {{ ref('int_team_match_performance') }}
),

teams as (
    select * from {{ ref('stg_soccer__team') }}
),

leagues as (
    select * from {{ ref('stg_soccer__league') }}
),

season_stats as (
    select
        season,
        league_id,
        team_id,
        count(*)                                           as matches_played,
        sum(case when points_earned = 3 then 1 else 0 end) as wins,
        sum(case when points_earned = 1 then 1 else 0 end) as draws,
        sum(case when points_earned = 0 then 1 else 0 end) as losses,
        sum(goals_scored)                                  as total_goals_scored,
        sum(goals_conceded)                                as total_goals_conceded,
        sum(goals_scored) - sum(goals_conceded)            as goal_difference,
        sum(points_earned)                                 as total_points
    from team_performance
    group by 1, 2, 3
),

final_standings as (
    select
        rank() over (
            partition by s.season, s.league_id
            order by s.total_points desc,
                     s.goal_difference desc,
                     s.total_goals_scored desc
        ) as league_position,
        s.season,
        s.league_id,
        l.league_name,
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
    inner join teams   t on s.team_id   = t.team_id
    inner join leagues l on s.league_id = l.league_id
)

select * from final_standings
order by season desc, league_id, league_position asc