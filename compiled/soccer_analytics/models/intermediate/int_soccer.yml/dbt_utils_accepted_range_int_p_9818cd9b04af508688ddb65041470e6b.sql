

with meet_condition as(
  select *
  from SOCCER_DB.ci_pr_test_intermediate.int_player_age_analysis
),

validation_errors as (
  select *
  from meet_condition
  where
    -- never true, defaults to an empty result set. Exists to ensure any combo of the `or` clauses below succeeds
    1 = 2
    -- records with a value >= min_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not age_at_rating >= 14
    -- records with a value <= max_value are permitted. The `not` flips this to find records that don't meet the rule.
    or not age_at_rating <= 50
)

select *
from validation_errors

