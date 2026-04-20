with players as (
    select * from {{ ref('stg_soccer__player') }}
),

attributes as (
    select * from {{ ref('stg_soccer__player_attributes') }}
),

age_calc as (
    select
        a.attribute_pk,
        p.player_id,
        p.player_name,
        p.birthday_date,
        a.rating_date,
        a.overall_rating,

        datediff('year', p.birthday_date, a.rating_date)
        - case
              when to_char(a.rating_date,   'MMDD')
                 < to_char(p.birthday_date, 'MMDD')
              then 1
              else 0
          end as age_at_rating
    from players p
    inner join attributes a on p.player_id = a.player_id
    -- Data quality filter: exclude default/malformed rating_date entries from the source system
    where a.rating_date >= '2008-01-01'
)

select
    attribute_pk,
    player_id,
    player_name,
    birthday_date,
    rating_date,
    overall_rating,
    age_at_rating
from age_calc