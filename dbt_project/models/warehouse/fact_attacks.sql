{{ config(
    materialized='table',
    post_hook=[
        "ALTER TABLE {{ this }} ADD PRIMARY KEY (event_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_date FOREIGN KEY (date_id) REFERENCES {{ ref('dim_date') }} (date_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_location FOREIGN KEY (location_id) REFERENCES {{ ref('dim_location') }} (location_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_attack FOREIGN KEY (attack_id) REFERENCES {{ ref('dim_attack') }} (attack_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_target FOREIGN KEY (target_id) REFERENCES {{ ref('dim_target') }} (target_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_perpetrator FOREIGN KEY (perpetrator_id) REFERENCES {{ ref('dim_perpetrator') }} (perpetrator_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_weapon FOREIGN KEY (weapon_id) REFERENCES {{ ref('dim_weapon') }} (weapon_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_narrative FOREIGN KEY (narrative_id) REFERENCES {{ ref('dim_narrative') }} (narrative_id)",
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_economy FOREIGN KEY (economy_id) REFERENCES {{ ref('dim_economy') }} (economy_id)"
    ]
) }}

with source as (
    select * from {{ ref('stg_attacks') }}
),

countries as (
    select country_id, country_name 
    from {{ ref('dim_country') }}
),

economies as (
    select economy_id, country_id, year 
    from {{ ref('dim_economy') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['source.year', 'source.month', 'source.day']) }} as date_id,
        {{ dbt_utils.generate_surrogate_key(['source.country_name', 'source.region_name', 'source.city_name', 'source.latitude', 'source.longitude']) }} as location_id,
        {{ dbt_utils.generate_surrogate_key(['source.attack_type']) }} as attack_id,
        {{ dbt_utils.generate_surrogate_key(['source.target_type']) }} as target_id,
        {{ dbt_utils.generate_surrogate_key(['source.group_name']) }} as perpetrator_id,
        {{ dbt_utils.generate_surrogate_key(['source.weapon_type']) }} as weapon_id,
        {{ dbt_utils.generate_surrogate_key(['source.event_id']) }} as narrative_id,
        
        e.economy_id,

        source.event_id,
        source.killed,
        source.wounded,
        (source.killed + source.wounded) as total_casualties,
        1 as incident_count

    from source
    left join countries c 
        on source.country_name = c.country_name
    
    left join economies e 
        on c.country_id = e.country_id 
        and source.year = e.year
)

select * from final