
    
    

select
    team_attribute_pk as unique_field,
    count(*) as n_records

from SOCCER_DB.SNAPSHOTS.snp_team_attributes
where team_attribute_pk is not null
group by team_attribute_pk
having count(*) > 1


