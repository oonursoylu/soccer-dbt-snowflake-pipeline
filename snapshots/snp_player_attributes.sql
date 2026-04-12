{% snapshot snp_player_attributes %}

{{
    config(
      target_database='SOCCER_DB',
      target_schema='SNAPSHOTS',
      unique_key='attribute_pk',
      strategy='timestamp',
      updated_at='rating_date'
    )
}}

select * from {{ ref('stg_soccer__player_attributes') }}

{% endsnapshot %}