{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (country_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['country_name']) }} as country_id,
    country_name
from {{ ref('stg_attacks') }}