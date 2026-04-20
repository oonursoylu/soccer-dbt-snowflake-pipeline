/*
  Singular test: within a (season, league), total wins must equal total losses,
  because every match produces exactly one winner and one loser (draws are
  not counted). A deviation indicates an unpivot bug or missing/duplicate rows.
*/
with per_season as (
    select
        season,
        league_id,
        sum(wins)   as total_wins,
        sum(losses) as total_losses
    from SOCCER_DB.ci_pr_test_marts.mart_league_standings
    group by 1, 2
)

select *
from per_season
where total_wins <> total_losses