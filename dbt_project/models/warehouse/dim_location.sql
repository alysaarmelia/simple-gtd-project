{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (location_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_location_country FOREIGN KEY (country_id) REFERENCES {{ ref('dim_country') }} (country_id)"
    ]
) }}

with distinct_locations as (
    select distinct
        country_name,
        region_name,
        city_name,
        latitude,
        longitude
    from {{ ref('stg_attacks') }}
),

countries as (
    select country_id, country_name 
    from {{ ref('dim_country') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['l.country_name', 'l.region_name', 'l.city_name', 'l.latitude', 'l.longitude']) }} as location_id,
    
    c.country_id,

    -- [PENTING] Tambahkan kolom ini agar Fact Table bisa membacanya
    l.country_name,
    
    l.region_name,
    l.city_name,
    l.latitude,
    l.longitude

from distinct_locations l

left join countries c on l.country_name = c.country_name