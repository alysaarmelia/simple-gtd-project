{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as narrative_id,
    event_id as original_event_id,
    summary as incident_summary
from {{ ref('stg_attacks') }}
where summary is not null