{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (narrative_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['event_id']) }} as narrative_id,
    event_id as original_event_id,
    -- Gunakan COALESCE agar tidak ada nilai null jika diinginkan, 
    -- atau biarkan null tapi hapus filter WHERE di bawah
    coalesce(summary, 'No summary available') as incident_summary
from {{ ref('stg_attacks') }}
-- HAPUS filter 'where summary is not null' agar semua event_id masuk ke dimensi ini