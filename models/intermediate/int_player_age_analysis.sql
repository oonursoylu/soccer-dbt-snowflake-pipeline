with players as (
    select * from {{ ref('stg_soccer__player') }}
),

attributes as (
    select * from {{ ref('stg_soccer__player_attributes') }}
),

age_calc as (
    select
        p.player_id,
        p.player_name,
        p.birthday_date, 
        a.rating_date,
        a.overall_rating,
        -- Using birthday_date for the calculation
        datediff(year, p.birthday_date, a.rating_date) as age_at_rating
    from players p
    inner join attributes a on p.player_id = a.player_id
)


select 
    player_id,
    player_name,
    birthday_date as birthday, 
    rating_date,
    overall_rating,
    age_at_rating
from age_calc