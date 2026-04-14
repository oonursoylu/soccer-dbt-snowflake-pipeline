/* This model identifies player career milestones by separating window functions into CTEs to avoid Snowflake nesting errors. */

with player_snapshots as (
    select * from {{ ref('snp_player_attributes') }}
),

players as (
    select * from {{ ref('stg_soccer__player') }}
),

-- Step 1: Calculate basic career stats and the peak rating value
base_career_stats as (
    select
        player_id,
        rating_date,
        overall_rating,
        dbt_valid_from,
        dbt_valid_to,
        -- Get the first rating chronologically
        first_value(overall_rating) over (
            partition by player_id 
            order by rating_date asc
        ) as initial_rating,
        -- Get the highest rating ever achieved (this is our first window function)
        max(overall_rating) over (partition by player_id) as peak_rating_value,
        -- Get the career start date
        min(rating_date) over (partition by player_id) as career_start_date,
        count(*) over (partition by player_id) as total_updates
    from player_snapshots
),

-- Step 2: Use the pre-calculated peak_rating_value to find peak milestones
peak_date_stats as (
    select
        *,
        -- Now we use the column peak_rating_value instead of a nested max() over()
        max(case when overall_rating = peak_rating_value then rating_date end) 
            over (partition by player_id) as peak_date,
            
        -- SAFE DURATION LOGIC: Find the exact first and last day of the peak rating.
        min(case when overall_rating = peak_rating_value then dbt_valid_from end) 
            over (partition by player_id) as peak_start_date,
        max(case when overall_rating = peak_rating_value then coalesce(dbt_valid_to, '2016-06-30'::date) end) 
            over (partition by player_id) as peak_end_date
    from base_career_stats
),

-- Step 3: Deduplicate to keep one row per player
unique_career_stats as (
    select *
    from peak_date_stats
    -- Anchoring the record to the earliest observation point
    qualify row_number() over (partition by player_id order by rating_date asc) = 1
),

-- Final output with age and growth logic
final as (
    select
        p.player_name,
        p.birthday_date,
        s.initial_rating,
        s.peak_rating_value as peak_rating,
        -- Inline growth calculation
        round(
            (s.peak_rating_value - s.initial_rating) * 100.0 / 
            nullif(s.initial_rating, 0), 2
        ) as growth_percentage,
        datediff('year', p.birthday_date, s.career_start_date) as age_at_start,
        datediff('year', p.birthday_date, s.peak_date) as age_at_peak,
        -- Calculate precise months spent at peak without overlapping SUM errors
        datediff('month', s.peak_start_date, s.peak_end_date) as peak_duration_months,
        s.total_updates,
        s.career_start_date,
        s.peak_date as peak_rating_date
    from unique_career_stats s
    join players p on s.player_id = p.player_id
    where s.initial_rating != s.peak_rating_value
      and s.total_updates > 1
)

select * from final
order by growth_percentage desc