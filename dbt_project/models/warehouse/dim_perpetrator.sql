{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['group_name']) }} as perpetrator_id,
    group_name
from {{ ref('stg_attacks') }}