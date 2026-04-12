/* Test: A player's peak rating mathematically cannot be lower than their initial rating.
  If this fails, it indicates an issue with the chronological ordering of snapshots.
*/
select
    player_name,
    initial_rating,
    peak_rating
from {{ ref('mart_player_performance_evolution') }}
where peak_rating < initial_rating