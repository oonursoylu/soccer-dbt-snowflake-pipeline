/* This model analyzes team tactical DNA by aggregating historical attributes 
   and classifying playstyles using reusable macros. */

with team_snapshots as (
    select * from SOCCER_DB.SNAPSHOTS.snp_team_attributes
),

teams as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__team
),

-- Step 1: Aggregate tactical scores and ensure data quality
team_tactical_summary as (
    select
        team_id,
        avg(build_up_play_speed) as avg_speed,
        avg(build_up_play_passing) as avg_passing,
        avg(chance_creation_shooting) as avg_shooting,
        avg(defence_pressure) as avg_pressure,
        avg(defence_aggression) as avg_aggression,
        count(*) as observation_count
    from team_snapshots
    group by 1
),

-- Step 2: Apply the macros to generate classifications
tactical_classes as (
    select
        team_id,
        observation_count,
        round(avg_speed, 2) as speed_index,
        
    case
        when avg_speed is null then null
        when avg_speed >= 65   then 'High'
        when avg_speed >= 40   then 'Medium'
        else 'Low'
    end
 as speed_class,
        
        round(avg_passing, 2) as passing_index,
        
    case
        when avg_passing is null then null
        when avg_passing >= 65   then 'High'
        when avg_passing >= 40   then 'Medium'
        else 'Low'
    end
 as passing_class,
        
        round(avg_shooting, 2) as shooting_index,
        
    case
        when avg_shooting is null then null
        when avg_shooting >= 65   then 'High'
        when avg_shooting >= 40   then 'Medium'
        else 'Low'
    end
 as shooting_class,
        
        round(avg_pressure, 2) as pressure_index,
        
    case
        when avg_pressure is null then null
        when avg_pressure >= 65   then 'High'
        when avg_pressure >= 40   then 'Medium'
        else 'Low'
    end
 as pressure_class,
        
        round(avg_aggression, 2) as aggression_index,
        
    case
        when avg_aggression is null then null
        when avg_aggression >= 65   then 'High'
        when avg_aggression >= 40   then 'Medium'
        else 'Low'
    end
 as aggression_class
    from team_tactical_summary
    where observation_count >= 5 -- Statistical significance filter applied early
),

-- Step 3: Identify Archetype using the Macro Classifications (DRY Principle)
final as (
    select
        t.team_long_name as team_name,
        c.observation_count,
        c.speed_index,
        c.speed_class,
        c.passing_index,
        c.passing_class,
        c.shooting_index,
        c.shooting_class,
        c.pressure_index,
        c.pressure_class,
        c.aggression_index,
        c.aggression_class,


        case
            when c.pressure_class = 'High' and c.aggression_class = 'High'
                then 'High Pressing / Aggressive'
            when c.speed_class    = 'Low'  and c.passing_class    = 'High'
                then 'Tiki-Taka / Possession'
            when c.speed_class    = 'High' and c.shooting_class   = 'High'
                then 'Fast Counter-Attack'
            else 'Balanced'
        end as tactical_archetype
        
    from tactical_classes c
    join teams t on c.team_id = t.team_id
)

select * from final
order by speed_index desc