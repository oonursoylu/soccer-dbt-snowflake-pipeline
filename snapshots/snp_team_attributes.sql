{% snapshot snp_team_attributes %}

{{
    config(
      target_database='SOCCER_DB',
      target_schema='SNAPSHOTS',
      unique_key='team_attribute_pk',
      strategy='timestamp',
      updated_at='rating_date'
    )
}}

select * from {{ ref('stg_soccer__team_attributes') }}

{% endsnapshot %}