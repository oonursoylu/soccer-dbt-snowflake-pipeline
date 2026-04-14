
    
    

select
    player_id as unique_field,
    count(*) as n_records

from SOCCER_DB.ci_pr_test_staging.stg_soccer__player
where player_id is not null
group by player_id
having count(*) > 1


