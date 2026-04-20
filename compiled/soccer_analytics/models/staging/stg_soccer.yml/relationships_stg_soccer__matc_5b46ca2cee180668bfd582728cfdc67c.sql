
    
    

with child as (
    select away_team_id as from_field
    from SOCCER_DB.ci_pr_test_staging.stg_soccer__matches
    where away_team_id is not null
),

parent as (
    select team_id as to_field
    from SOCCER_DB.ci_pr_test_staging.stg_soccer__team
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


