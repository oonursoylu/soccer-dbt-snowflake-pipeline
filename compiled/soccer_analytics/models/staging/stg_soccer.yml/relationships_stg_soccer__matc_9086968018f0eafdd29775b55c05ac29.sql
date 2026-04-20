
    
    

with child as (
    select league_id as from_field
    from SOCCER_DB.ci_pr_test_staging.stg_soccer__matches
    where league_id is not null
),

parent as (
    select league_id as to_field
    from SOCCER_DB.ci_pr_test_staging.stg_soccer__league
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


