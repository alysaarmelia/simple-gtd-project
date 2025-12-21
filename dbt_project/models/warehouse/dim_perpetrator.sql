{{ config(
    materialized='table',
    post_hook=["ALTER TABLE {{ this }} ADD PRIMARY KEY (perpetrator_id)"]
) }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['group_name']) }} as perpetrator_id,
    group_name
from {{ ref('stg_attacks') }}
where group_name is not null