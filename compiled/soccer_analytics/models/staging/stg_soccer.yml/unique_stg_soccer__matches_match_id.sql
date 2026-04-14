
    
    

select
    match_id as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test_staging.stg_soccer__matches
where match_id is not null
group by match_id
having count(*) > 1


