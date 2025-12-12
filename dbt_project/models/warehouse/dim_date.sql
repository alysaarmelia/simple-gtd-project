{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['year', 'month', 'day']) }} as date_id,
    year,
    month,
    day,

    case
        when month = 0 or day = 0 then null
        else cast(concat(year, '-', month, '-', day) as date)
    end as full_date
from {{ ref('stg_attacks') }}