



select
    1
from SOCCER_DB.ci_pr_test_marts.mart_team_betting_predictability

where not(unpredictability_index between 0 and 100)

