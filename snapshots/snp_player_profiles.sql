{% snapshot snp_player_profiles %}

{{
    config(
      target_database='SOCCER_DB',
      target_schema='SNAPSHOTS',
      unique_key='player_id',
      strategy='check',
      check_cols=['height_cm', 'weight_lbs']
    )
}}

select * from {{ ref('stg_soccer__player') }}

{% endsnapshot %}