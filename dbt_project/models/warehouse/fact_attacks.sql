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
        "ALTER TABLE {{ this }} ADD CONSTRAINT fk_narrative FOREIGN KEY (narrative_id) REFERENCES {{ ref('dim_narrative') }} (narrative_id)"
    ]
) }}

with source as (
    select * from {{ ref('stg_attacks') }}
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['year', 'month', 'day']) }} as date_id,
        {{ dbt_utils.generate_surrogate_key(['country_name', 'region_name', 'city_name', 'latitude', 'longitude']) }} as location_id,
        {{ dbt_utils.generate_surrogate_key(['attack_type']) }} as attack_id,
        {{ dbt_utils.generate_surrogate_key(['target_type']) }} as target_id,
        {{ dbt_utils.generate_surrogate_key(['group_name']) }} as perpetrator_id,
        {{ dbt_utils.generate_surrogate_key(['weapon_type']) }} as weapon_id,
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as narrative_id,

        event_id,
        killed,
        wounded,
        (killed + wounded) as total_casualties,
        1 as incident_count

    from source
)

select * from final