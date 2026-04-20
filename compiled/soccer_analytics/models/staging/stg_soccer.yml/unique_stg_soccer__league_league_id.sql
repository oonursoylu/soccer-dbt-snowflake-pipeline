
    
    

select
    league_id as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test_staging.stg_soccer__league
where league_id is not null
group by league_id
having count(*) > 1


