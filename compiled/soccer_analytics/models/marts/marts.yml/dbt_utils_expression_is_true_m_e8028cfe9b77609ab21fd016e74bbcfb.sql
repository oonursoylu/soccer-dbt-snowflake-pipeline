



select
    1
from SOCCER_DB.ci_pr_test_marts.mart_team_betting_predictability

where not(full_upsets = favorite_fails + underdog_wins)

