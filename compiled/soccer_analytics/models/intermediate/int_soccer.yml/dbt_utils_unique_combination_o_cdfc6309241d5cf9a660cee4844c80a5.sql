





with validation_errors as (

    select
        match_id, team_id
    from SOCCER_DB.ci_pr_test_intermediate.int_team_match_performance
    group by match_id, team_id
    having count(*) > 1

)

select *
from validation_errors


