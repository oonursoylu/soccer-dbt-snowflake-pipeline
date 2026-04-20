
    
    

with all_values as (

    select
        venue as value_field,
        count(*) as n_records

    from SOCCER_DB.ci_pr_test_intermediate.int_team_match_performance
    group by venue

)

select *
from all_values
where value_field not in (
    'Home','Away'
)


