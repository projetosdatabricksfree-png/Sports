-- =============================================================================
-- snp_team_roster — SCD Type 2: rastreia mudanças no elenco dos times
-- =============================================================================
{% snapshot snp_team_roster %}

{{
  config(
    target_schema = 'snapshots',
    unique_key    = 'player_id',
    strategy      = 'timestamp',
    updated_at    = 'updated_at',
    invalidate_hard_deletes = true
  )
}}

select * from {{ source('raw', 'team_roster') }}

{% endsnapshot %}
