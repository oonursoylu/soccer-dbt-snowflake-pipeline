/* This model provides a team-level analysis of betting predictability. 
   Uses a Team-Centric macro for consistent logic across home and away matches.
*/

with matches as (
    select * from {{ ref('stg_soccer__matches') }}
),

teams as (
    select * from {{ ref('stg_soccer__team') }}
),

leagues as (
    select * from {{ ref('stg_soccer__league') }}
),

-- Creating a long-format table where each team in a match is a separate row
team_match_outcomes as (
    -- Home team perspective
    select
        season,
        league_id,
        home_team_id as team_id,
        odds_home_win as team_odds,
        odds_away_win as opponent_odds,
        odds_draw,
        case 
            when home_team_goal > away_team_goal then 'W'
            when home_team_goal < away_team_goal then 'L'
            else 'D'
        end as team_result,
        -- Clean Team-Centric call
        {{ is_favorite_upset('odds_home_win', 'odds_away_win', 'odds_draw', "case when home_team_goal > away_team_goal then 'W' when home_team_goal < away_team_goal then 'L' else 'D' end") }} as betting_outcome
    from matches
    
    union all
    
    -- Away team perspective
    select
        season,
        league_id,
        away_team_id as team_id,
        odds_away_win as team_odds,
        odds_home_win as opponent_odds,
        odds_draw,
        case 
            when away_team_goal > home_team_goal then 'W'
            when away_team_goal < home_team_goal then 'L'
            else 'D'
        end as team_result,
        -- Consistent call, no parameter swapping needed
        {{ is_favorite_upset('odds_away_win', 'odds_home_win', 'odds_draw', "case when away_team_goal > home_team_goal then 'W' when away_team_goal < home_team_goal then 'L' else 'D' end") }} as betting_outcome
    from matches
),

-- Aggregating unpredictable events (Favorite Fails and Giant Killers)
team_behavior as (
    select
        team_id,
        season,
        league_id,
        count(*) as total_matches,
        sum(case when betting_outcome = 'Upset' and team_result != 'W' then 1 else 0 end) as favorite_fails,
        sum(case when betting_outcome = 'Upset' and team_result = 'W' then 1 else 0 end) as underdog_wins,
        sum(case when betting_outcome = 'High Risk' then 1 else 0 end) as high_risk_matches
    from team_match_outcomes
    where team_odds is not null
    group by 1, 2, 3
),

-- Calculating the final Unpredictability Index
final as (
    select
        l.league_name,
        t.team_long_name as team_name,
        b.season,
        b.total_matches,
        b.favorite_fails,
        b.underdog_wins,
        b.high_risk_matches,
        (b.favorite_fails + b.underdog_wins) as total_unpredictable_events,
        round(
            (b.favorite_fails + b.underdog_wins) * 100.0 / 
            nullif(b.total_matches - b.high_risk_matches, 0), 2
        ) as unpredictability_index
    from team_behavior b
    join teams t on b.team_id = t.team_id
    join leagues l on b.league_id = l.league_id
    where b.total_matches >= 10
)

select * from final
order by unpredictability_index desc, total_matches desc