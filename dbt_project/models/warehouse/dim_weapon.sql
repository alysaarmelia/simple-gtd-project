{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (weapon_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['weapon_type']) }} as weapon_id,
    weapon_type
from {{ ref('stg_attacks') }}