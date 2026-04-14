
    
    

with all_values as (

    select
        tactical_archetype as value_field,
        count(*) as n_records

    from SOCCER_DB.ci_pr_test_marts.mart_team_tactical_dna
    group by tactical_archetype

)

select *
from all_values
where value_field not in (
    'Tiki-Taka / Possession','Fast Counter-Attack','High Pressing / Aggressive','Balanced'
)


