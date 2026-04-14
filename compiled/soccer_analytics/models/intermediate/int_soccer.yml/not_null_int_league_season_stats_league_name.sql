
    
    



with __dbt__cte__int_league_season_stats as (
with matches as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__matches
),

leagues as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__league
),

summary as (
    select
        l.league_name,
        m.season,
        -- I am calculating high-level season metrics
        count(m.match_id) as total_matches,
        sum(m.home_team_goal + m.away_team_goal) as total_goals,
        cast(avg(m.home_team_goal + m.away_team_goal) as decimal(5,2)) as avg_goals_per_match
    from matches m
    left join leagues l on m.league_id = l.league_id
    group by 1, 2
)

select * from summary
) select league_name
from __dbt__cte__int_league_season_stats
where league_name is null


