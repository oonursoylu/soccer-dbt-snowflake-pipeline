with players as (
    select * from {{ ref('stg_soccer__player') }}
),

attributes as (
    select * from {{ ref('stg_soccer__player_attributes') }}
),

-- Step 1: Base conversions (Imperial to Metric)
base_metrics as (
    select
        a.attribute_pk, -- ADDED: The crucial Primary Key to maintain granularity!
        p.player_id,
        p.player_name,
        p.birthday_date,
        a.rating_date,
        a.overall_rating,
        a.potential,
        a.preferred_foot,
        
        -- Physical and technical attributes
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

        -- Converting units ONCE
        cast((p.weight_lbs * 0.453592) as decimal(5,2)) as weight_kg,
        cast((p.height_cm / 100.0) as decimal(5,2)) as height_m

    from players p
    -- Switched to INNER JOIN: If a player has no attributes, we can't show their stats anyway
    inner join attributes a on p.player_id = a.player_id
),

-- Step 2: Calculate dependent metrics using the clean base metrics (DRY Principle)
final_stats as (
    select 
        *,
        -- Now BMI calculation is incredibly easy to read
        cast((weight_kg / power(height_m, 2)) as decimal(5,2)) as bmi
    from base_metrics
)

select * from final_stats