
    
    

select
    team_id as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test_staging.stg_soccer__team
where team_id is not null
group by team_id
having count(*) > 1


