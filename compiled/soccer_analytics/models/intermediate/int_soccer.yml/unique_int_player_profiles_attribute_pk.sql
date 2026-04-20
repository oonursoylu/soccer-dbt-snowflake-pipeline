
    
    

select
    attribute_pk as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test_intermediate.int_player_profiles
where attribute_pk is not null
group by attribute_pk
having count(*) > 1


