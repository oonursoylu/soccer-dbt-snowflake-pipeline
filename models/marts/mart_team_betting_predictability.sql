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

-- Step 1: Unpivot to create a base team-centric view and calculate match results first
base_outcomes as (
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
        end as team_result
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
        end as team_result
    from matches
),

-- Step 2: Call the macro using clean column names (DRY and Readability)
team_match_outcomes as (
    select 
        *,
        -- Beautiful, clean macro call using the derived team_result column
        {{ is_favorite_upset('team_odds', 'opponent_odds', 'odds_draw', 'team_result') }} as betting_outcome
    from base_outcomes
),

-- Step 3: Aggregate unpredictable events (Favorite Fails and Giant Killers)
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

-- Step 4: Calculate the final Unpredictability Index
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
        -- Calculate percentage of upsets, excluding high-risk (unpredictable) matches from the denominator
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