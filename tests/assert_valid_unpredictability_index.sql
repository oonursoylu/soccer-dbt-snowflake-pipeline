/* Test: The unpredictability index is a percentage and must fall strictly between 0 and 100.
Reference: Pointing to the correct mart model name to ensure proper lineage.
*/

select
    team_name,
    season,
    unpredictability_index
from {{ ref('mart_team_betting_predictability') }} 
where unpredictability_index < 0 or unpredictability_index > 100