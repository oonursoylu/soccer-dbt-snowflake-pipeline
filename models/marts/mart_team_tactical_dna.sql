/* This model analyzes team tactical DNA by aggregating historical attributes 
   and classifying playstyles using reusable macros. */

with team_snapshots as (
    select * from {{ ref('snp_team_attributes') }}
),

teams as (
    select * from {{ ref('stg_soccer__team') }}
),

-- Aggregating tactical scores and ensuring data quality through observation counts
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

-- Final classification and playstyle identification
final as (
    select
        t.team_long_name as team_name,
        s.observation_count,
        
        -- Play Speed
        round(s.avg_speed, 2) as speed_index,
        {{ classify_tactical_score('s.avg_speed') }} as speed_class,
        
        -- Passing Style
        round(s.avg_passing, 2) as passing_index,
        {{ classify_tactical_score('s.avg_passing') }} as passing_class,
        
        -- Shooting / Chance Creation
        round(s.avg_shooting, 2) as shooting_index,
        {{ classify_tactical_score('s.avg_shooting') }} as shooting_class,
        
        -- Defensive DNA
        round(s.avg_pressure, 2) as pressure_index,
        {{ classify_tactical_score('s.avg_pressure') }} as pressure_class,
        
        round(s.avg_aggression, 2) as aggression_index,
        {{ classify_tactical_score('s.avg_aggression') }} as aggression_class,

        -- Tactical Archetype Identification
        case 
            when s.avg_speed < 40 and s.avg_passing >= 65 then 'Tiki-Taka / Possession'
            when s.avg_speed >= 65 and s.avg_shooting >= 60 then 'Fast Counter-Attack'
            when s.avg_pressure >= 65 and s.avg_aggression >= 65 then 'High Pressing / Aggressive'
            else 'Balanced'
        end as tactical_archetype
    from team_tactical_summary s
    join teams t on s.team_id = t.team_id
    -- Filtering for statistical significance (minimum 5 seasons/updates)
    where s.observation_count >= 5
)

select * from final
order by speed_index desc