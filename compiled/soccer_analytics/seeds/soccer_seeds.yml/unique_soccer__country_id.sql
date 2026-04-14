
    
    

select
    id as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test.soccer__country
where id is not null
group by id
having count(*) > 1


