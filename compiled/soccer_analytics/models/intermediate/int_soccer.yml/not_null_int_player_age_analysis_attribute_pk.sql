
    
    



with __dbt__cte__int_player_age_analysis as (
with players as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__player
),

attributes as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__player_attributes
),

age_calc as (
    select
        a.attribute_pk, -- Bringing the primary key from attributes
        p.player_id,
        p.player_name,
        p.birthday_date, 
        a.rating_date,
        a.overall_rating,
        -- Calculating player age based on the specific rating date
        datediff(year, p.birthday_date, a.rating_date) as age_at_rating
    from players p
    inner join attributes a on p.player_id = a.player_id
)

select 
    attribute_pk,
    player_id,
    player_name,
    birthday_date as birthday, 
    rating_date,
    overall_rating,
    age_at_rating
from age_calc
) select attribute_pk
from __dbt__cte__int_player_age_analysis
where attribute_pk is null


