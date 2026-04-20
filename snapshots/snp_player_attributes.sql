{% snapshot snp_player_attributes %}

{{
    config(
      target_database='SOCCER_DB',
      target_schema='SNAPSHOTS',
      unique_key='player_id',
      strategy='timestamp',
      updated_at='rating_date',
      invalidate_hard_deletes=True
    )
}}

select * from {{ ref('stg_soccer__player_attributes') }}

{% endsnapshot %}