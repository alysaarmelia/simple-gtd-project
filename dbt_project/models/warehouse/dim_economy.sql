{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (economy_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_dim_economy_country FOREIGN KEY (country_id) REFERENCES {{ ref('dim_country') }} (country_id)"
    ]
) }}

with economy as (
    select * from {{ ref('stg_economy') }}
),

countries as (
    select * from {{ ref('dim_country') }}
),

annual_economy as (
    select
        country_name,
        year,
        AVG(property_index) as property_index 
    from economy
    group by 1, 2
)

select
    {{ dbt_utils.generate_surrogate_key(['e.country_name', 'e.year']) }} as economy_id,
    c.country_id,
    e.year,
    e.property_index
from annual_economy e
join countries c on e.country_name = c.country_name