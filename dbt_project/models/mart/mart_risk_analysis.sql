{{ config(materialized='table') }}

with security_metrics as (

    select 
        l.country_name,
        d.year,
        sum(f.incident_count) as total_attacks,
        sum(f.killed) as total_killed
    from {{ ref('fact_attacks') }} f
    join {{ ref('dim_location') }} l on f.location_id = l.location_id
    join {{ ref('dim_date') }} d on f.date_id = d.date_id
    group by l.country_name, d.year
),

economic_metrics as (

    select 
        country_name,
        year,
        property_index
    from {{ ref('fact_economy') }}
),

final as (
    select
        s.country_name,
        s.year,
        s.total_attacks,
        s.total_killed,
        e.property_index,
        
        case
            when s.total_attacks < 5 and e.property_index > 100 then 'Safe Haven (Growth)'
            when s.total_attacks > 50 and e.property_index > 100 then 'High Risk (Bubble)'
            else 'Neutral'
        end as investment_signal

    from security_metrics s
    inner join economic_metrics e 
        on s.country_name = e.country_name 
        and s.year = e.year
)

select * from final