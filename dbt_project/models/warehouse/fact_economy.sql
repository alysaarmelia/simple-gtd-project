{{ config(materialized='table') }}

select
    -- Kita tidak menggunakan surrogate key (ID) ke dim_location 
    -- karena data ini levelnya Negara, bukan Kota.
    country_name,
    year,
    property_index

from {{ ref('stg_economy') }}