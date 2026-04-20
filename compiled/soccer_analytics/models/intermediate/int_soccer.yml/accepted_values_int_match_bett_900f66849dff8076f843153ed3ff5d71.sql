
    
    

with all_values as (

    select
        prediction_status as value_field,
        count(*) as n_records

    from SOCCER_DB.ci_pr_test_intermediate.int_match_betting_analysis
    group by prediction_status

)

select *
from all_values
where value_field not in (
    'Correct','Wrong','Ambiguous'
)


