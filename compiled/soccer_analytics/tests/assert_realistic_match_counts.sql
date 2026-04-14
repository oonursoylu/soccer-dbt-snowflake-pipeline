/* Test: A team should not play an unrealistic number of matches in a single league season.
  Threshold set to 50 to account for maximum possible league sizes.
*/
select
    season,
    league_id,
    team_name,
    matches_played
from SOCCER_DB.ci_pr_test_marts.mart_league_standings
where matches_played > 50