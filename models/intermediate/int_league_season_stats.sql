with matches as (
    select * from {{ ref('stg_soccer__matches') }}
),

leagues as (
    select * from {{ ref('stg_soccer__league') }}
),

summary as (
    select
        l.league_name,
        m.season,
        -- I am calculating high-level season metrics
        count(m.match_id) as total_matches,
        sum(m.home_team_goal + m.away_team_goal) as total_goals,
        cast(avg(m.home_team_goal + m.away_team_goal) as decimal(10,2)) as avg_goals_per_match
    from matches m
    left join leagues l on m.league_id = l.league_id
    group by 1, 2
)

select * from summary