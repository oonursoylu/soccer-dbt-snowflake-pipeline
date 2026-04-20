
    
    

select
    country_id as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test_staging.stg_soccer__country
where country_id is not null
group by country_id
having count(*) > 1


