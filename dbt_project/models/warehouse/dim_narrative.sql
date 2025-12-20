{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (narrative_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as narrative_id,
    event_id as original_event_id,

    coalesce(summary, 'No summary available') as incident_summary
from {{ ref('stg_attacks') }}
