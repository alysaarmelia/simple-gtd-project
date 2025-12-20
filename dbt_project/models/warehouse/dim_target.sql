{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (target_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['target_type']) }} as target_id,
    target_type
from {{ ref('stg_attacks') }}