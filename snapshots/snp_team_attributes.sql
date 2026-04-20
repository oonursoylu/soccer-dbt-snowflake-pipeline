{% snapshot snp_team_attributes %}

{{
    config(
      target_database='SOCCER_DB',
      target_schema='SNAPSHOTS',
      unique_key='team_attribute_pk',
      strategy='check',
      check_cols=['build_up_play_speed', 'defence_pressure', 'chance_creation_shooting']
    )
}}

select * from {{ ref('stg_soccer__team_attributes') }}

{% endsnapshot %}