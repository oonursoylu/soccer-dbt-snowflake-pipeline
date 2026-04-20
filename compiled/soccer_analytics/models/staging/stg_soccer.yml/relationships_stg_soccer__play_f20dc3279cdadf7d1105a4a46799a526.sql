
    
    

with child as (
    select player_id as from_field
    from SOCCER_DB.ci_pr_test_staging.stg_soccer__player_attributes
    where player_id is not null
),

parent as (
    select player_id as to_field
    from SOCCER_DB.ci_pr_test_staging.stg_soccer__player
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


