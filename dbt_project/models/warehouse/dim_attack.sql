{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['attack_type']) }} as attack_id,
    attack_type
from {{ ref('stg_attacks') }}