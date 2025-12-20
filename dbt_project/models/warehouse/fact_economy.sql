{{ config(materialized='table') }}

select
    country_name,
    year,
    property_index
from {{ ref('stg_economy') }}