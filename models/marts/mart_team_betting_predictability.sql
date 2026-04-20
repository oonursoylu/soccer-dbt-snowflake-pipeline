with match_betting as (
    select * from {{ ref('int_match_betting_analysis') }}
),

teams as (
    select * from {{ ref('stg_soccer__team') }}
),

leagues as (
    select * from {{ ref('stg_soccer__league') }}
),


team_match_outcomes as (
    -- Home team perspective
    select
        season,
        league_id,
        home_team_id  as team_id,
        match_id,
        odds_home_win as team_odds,
        odds_away_win as opponent_odds,
        odds_draw,
        case
            when home_team_goal >  away_team_goal then 'W'
            when home_team_goal <  away_team_goal then 'L'
            else 'D'
        end as team_result,
        bookie_favorite
    from match_betting

    union all

    -- Away team perspective
    select
        season,
        league_id,
        away_team_id  as team_id,
        match_id,
        odds_away_win as team_odds,
        odds_home_win as opponent_odds,
        odds_draw,
        case
            when away_team_goal >  home_team_goal then 'W'
            when away_team_goal <  home_team_goal then 'L'
            else 'D'
        end as team_result,
        bookie_favorite
    from match_betting
),

classified as (
    select
        *,
        {{ is_favorite_upset('team_odds', 'opponent_odds', 'odds_draw', 'team_result') }}
            as betting_outcome
    from team_match_outcomes
),

team_behavior as (
    select
        team_id,
        season,
        league_id,
        count(*) as total_matches,

        -- Full upsets where the team was the favorite but lost outright
        sum(case when betting_outcome = 'Upset'
                  and team_result = 'L'
                 then 1 else 0 end) as favorite_fails,

        -- Full upsets where the team was the underdog and won
        sum(case when betting_outcome = 'Upset'
                  and team_result = 'W'
                 then 1 else 0 end) as underdog_wins,

        -- Half-surprises: draws that went against the betting prediction
        sum(case when betting_outcome = 'Draw Upset'
                 then 1 else 0 end) as draw_upsets,

        -- Matches where bookie had no clear prediction
        sum(case when betting_outcome = 'High Risk'
                 then 1 else 0 end) as high_risk_matches
    from classified
    group by 1, 2, 3
),

final as (
    select
        l.league_name,
        t.team_long_name as team_name,
        b.season,
        b.total_matches,
        b.favorite_fails,
        b.underdog_wins,
        b.draw_upsets,
        b.high_risk_matches,

        (b.favorite_fails + b.underdog_wins)                         as full_upsets,
        (b.favorite_fails + b.underdog_wins + b.draw_upsets)         as total_unpredictable_events,

        round(
            ((b.favorite_fails + b.underdog_wins) * 1.0
             + b.draw_upsets * 0.5) * 100.0
            / nullif(b.total_matches - b.high_risk_matches, 0),
            2
        ) as unpredictability_index

    from team_behavior b
    inner join teams   t on b.team_id   = t.team_id
    inner join leagues l on b.league_id = l.league_id
    where b.total_matches >= 10   -- Statistical significance floor
)

select * from final
order by unpredictability_index desc, total_matches desc