



select
    1
from SOCCER_DB.ci_pr_test_marts.mart_team_betting_predictability

where not(total_unpredictable_events = favorite_fails + underdog_wins + draw_upsets)

