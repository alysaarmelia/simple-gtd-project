{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (attack_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['attack_type']) }} as attack_id,
    attack_type
from {{ ref('stg_attacks') }}