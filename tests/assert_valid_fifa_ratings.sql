-- This test identifies players with ratings outside the valid FIFA range (1-99)
-- In dbt singular tests, we select the rows that FAIL our logic.
select
    player_name,
    initial_rating,
    peak_rating
from {{ ref('mart_player_performance_evolution') }}
where 
    -- Check if initial rating is unrealistic
    initial_rating < 1 
    or initial_rating > 100
    
    -- Check if peak rating is unrealistic
    or peak_rating < 1 
    or peak_rating > 100