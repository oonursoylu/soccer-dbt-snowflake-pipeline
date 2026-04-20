
    
    

select
    dbt_scd_id as unique_field,
    count(*) as n_records

from SOCCER_DB.SNAPSHOTS.snp_player_attributes
where dbt_scd_id is not null
group by dbt_scd_id
having count(*) > 1


