{% snapshot snp_player_attributes %}

{{
    config(
      target_database='SOCCER_DB',
      target_schema='SNAPSHOTS',
      unique_key='attribute_pk',
      strategy='check',
      check_cols=['overall_rating', 'potential']
    )
}}

select * from {{ ref('stg_soccer__player_attributes') }}

{% endsnapshot %}