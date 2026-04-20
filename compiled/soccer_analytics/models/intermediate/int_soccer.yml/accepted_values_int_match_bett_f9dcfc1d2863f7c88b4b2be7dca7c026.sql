
    
    

with all_values as (

    select
        bookie_favorite as value_field,
        count(*) as n_records

    from SOCCER_DB.ci_pr_test_intermediate.int_match_betting_analysis
    group by bookie_favorite

)

select *
from all_values
where value_field not in (
    'Home','Away','Draw','Tie'
)


