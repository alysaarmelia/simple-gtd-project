{{ config(materialized='table') }}

select distinct
    {{ dbt_utils.generate_surrogate_key(['country_name', 'region_name', 'city_name', 'latitude', 'longitude']) }} as location_id,
    country_name,
    region_name,
    city_name,
    latitude,
    longitude
from {{ ref('stg_attacks') }}