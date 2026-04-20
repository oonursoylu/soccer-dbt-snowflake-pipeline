



select
    1
from SOCCER_DB.ci_pr_test_marts.mart_league_standings

where not((wins * 3) + draws = total_points)

