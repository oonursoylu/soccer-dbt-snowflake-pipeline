-- This test identifies players with ratings outside the valid FIFA range (1-99)
select
    player_name,
    initial_rating,
    peak_rating
from SOCCER_DB.ci_pr_test_marts.mart_player_performance_evolution
where 
    -- Check if initial rating is unrealistic
    initial_rating < 1 
    or initial_rating > 99
    
    -- Check if peak rating is unrealistic
    or peak_rating < 1 
    or peak_rating > 99