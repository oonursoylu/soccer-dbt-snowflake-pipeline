with players as (
    select * from {{ ref('stg_soccer__player') }}
),

attributes as (
    select * from {{ ref('stg_soccer__player_attributes') }}
),

joined as (
    select
        p.player_id,
        p.player_name,
        p.birthday_date,
        p.height_cm,
        p.weight_lbs,
        a.rating_date,
        a.overall_rating,
        a.potential,
        a.preferred_foot,
        
        -- I am selecting physical and technical attributes for outfield players
        a.attacking_work_rate,
        a.defensive_work_rate,
        a.acceleration,
        a.sprint_speed,
        a.stamina,
        a.strength,
        a.finishing,
        a.short_passing,
        a.volleys,
        a.dribbling,
        a.long_passing,
        a.ball_control,
        a.interceptions,
        a.positioning,
        a.vision,
        a.marking,
        a.standing_tackle,

        -- I am converting weight from lbs to kg and calculating BMI. 
        -- Height is in cm, so I divide by 100 to get meters.
        cast((p.weight_lbs * 0.453592) as decimal(5,2)) as weight_kg,
        cast(((p.weight_lbs * 0.453592) / power((p.height_cm / 100.0), 2)) as decimal(5,2)) as bmi

    from players p
    left join attributes a on p.player_id = a.player_id
)

select * from joined