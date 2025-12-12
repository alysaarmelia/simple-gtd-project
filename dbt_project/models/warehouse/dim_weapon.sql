{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['weapon_type']) }} as weapon_id,
    weapon_type
from {{ ref('stg_attacks') }}