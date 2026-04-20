/*
  Player career evolution: compares initial vs peak rating and measures
  how long the player stayed at peak.

  NOTE: snapshot uses `check` strategy, so dbt_valid_from reflects snapshot
  run time, not the actual rating date. We use rating_date directly to
  compute career milestones.
*/

with player_snapshots as (
    select * from SOCCER_DB.SNAPSHOTS.snp_player_attributes
),

players as (
    select * from SOCCER_DB.ci_pr_test_staging.stg_soccer__player
),

-- Step 1: Basic career stats and peak rating value
base_career_stats as (
    select
        player_id,
        rating_date,
        overall_rating,

        first_value(overall_rating) over (
            partition by player_id
            order by rating_date asc
        ) as initial_rating,

        max(overall_rating) over (partition by player_id) as peak_rating_value,
        min(rating_date)    over (partition by player_id) as career_start_date,
        count(*)            over (partition by player_id) as total_updates
    from player_snapshots
),

-- Step 2: Find the date when peak rating was first and last reached
peak_date_stats as (
    select
        *,
        min(case when overall_rating = peak_rating_value then rating_date end)
            over (partition by player_id) as peak_start_date,

        max(case when overall_rating = peak_rating_value then rating_date end)
            over (partition by player_id) as peak_end_date
    from base_career_stats
),

-- Step 3: One row per player (earliest observation)
unique_career_stats as (
    select *
    from peak_date_stats
    qualify row_number() over (
        partition by player_id
        order by rating_date asc
    ) = 1
),

-- Step 4: Final projection
final as (
    select
        p.player_name,
        p.birthday_date,
        s.initial_rating,
        s.peak_rating_value as peak_rating,

        round(
            (s.peak_rating_value - s.initial_rating) * 100.0
            / nullif(s.initial_rating, 0),
            2
        ) as growth_percentage,

        datediff('year', p.birthday_date, s.career_start_date)
        - case
              when to_char(s.career_start_date, 'MMDD')
                 < to_char(p.birthday_date,     'MMDD')
              then 1 else 0
          end as age_at_start,

        datediff('year', p.birthday_date, s.peak_end_date)
        - case
              when to_char(s.peak_end_date,  'MMDD')
                 < to_char(p.birthday_date,  'MMDD')
              then 1 else 0
          end as age_at_peak,

        -- Duration from first reaching peak to last observation at peak.
        -- Uses rating_date (not dbt_valid_from) for correct semantics.
        datediff('month', s.peak_start_date, s.peak_end_date) as peak_duration_months,

        s.total_updates,
        s.career_start_date,
        s.peak_end_date as peak_rating_date

    from unique_career_stats s
    inner join players p on s.player_id = p.player_id
    where s.initial_rating  <> s.peak_rating_value
      and s.total_updates    > 1
)

select * from final
order by growth_percentage desc