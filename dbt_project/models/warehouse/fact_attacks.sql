{{ config(materialized='table') }}

with source as (
    select * from {{ ref('stg_attacks') }}
),

final as (
    select
        -- Foreign Keys ke Dimensi (Perbaikan sintaks disini)
        {{ dbt_utils.generate_surrogate_key(['year', 'month', 'day']) }} as date_id,
        {{ dbt_utils.generate_surrogate_key(['country_name', 'region_name', 'city_name', 'latitude', 'longitude']) }} as location_id,
        {{ dbt_utils.generate_surrogate_key(['attack_type']) }} as attack_id,
        {{ dbt_utils.generate_surrogate_key(['target_type']) }} as target_id,
        {{ dbt_utils.generate_surrogate_key(['group_name']) }} as perpetrator_id,
        {{ dbt_utils.generate_surrogate_key(['weapon_type']) }} as weapon_id,
        {{ dbt_utils.generate_surrogate_key(['event_id']) }} as narrative_id,

        -- Degenerate Dimension
        event_id,

        -- Metrics
        killed,
        wounded,
        (killed + wounded) as total_casualties,
        1 as incident_count

    from source
)

select * from final