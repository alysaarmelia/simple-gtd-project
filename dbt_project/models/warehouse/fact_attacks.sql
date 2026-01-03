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

-- 1. PANGGIL SEMUA TABEL DIMENSI
dim_country as (select country_id, country_name from {{ ref('dim_country') }}),
dim_economy as (select economy_id, country_id, year from {{ ref('dim_economy') }}),
dim_date as (select date_id, year, month, day from {{ ref('dim_date') }}),
dim_location as (select location_id, country_name, region_name, city_name, latitude, longitude from {{ ref('dim_location') }}),
dim_attack as (select attack_id, attack_type from {{ ref('dim_attack') }}),
dim_target as (select target_id, target_type from {{ ref('dim_target') }}),
dim_perpetrator as (select perpetrator_id, group_name from {{ ref('dim_perpetrator') }}),
dim_weapon as (select weapon_id, weapon_type from {{ ref('dim_weapon') }}),
dim_narrative as (select narrative_id, original_event_id from {{ ref('dim_narrative') }}),

final as (
    select
        -- 2. AMBIL ID DARI HASIL JOIN (BUKAN GENERATE ULANG)
        d.date_id,
        l.location_id,
        a.attack_id,
        t.target_id,
        p.perpetrator_id,
        w.weapon_id,
        n.narrative_id,
        e.economy_id,

        -- Metrics dari Source
        source.event_id,
        source.killed,
        source.wounded,
        (source.killed + source.wounded) as total_casualties,
        1 as incident_count

    from source
    
    -- 3. JOIN KE DIMENSI BERDASARKAN "NATURAL KEY"
    
    -- Join Location
    left join dim_location l 
        on source.country_name = l.country_name
        and source.region_name = l.region_name
        and source.city_name = l.city_name
        and source.latitude = l.latitude 
        and source.longitude = l.longitude

    -- Join Date
    left join dim_date d 
        on source.year = d.year 
        and source.month = d.month 
        and source.day = d.day

    -- Join Dimensi Lainnya
    left join dim_attack a on source.attack_type = a.attack_type
    left join dim_target t on source.target_type = t.target_type
    left join dim_perpetrator p on source.group_name = p.group_name
    left join dim_weapon w on source.weapon_type = w.weapon_type
    left join dim_narrative n on source.event_id = n.original_event_id
    
    -- Join Economy (Via Country agar konsisten)
    left join dim_country c on source.country_name = c.country_name
    left join dim_economy e 
        on c.country_id = e.country_id 
        and source.year = e.year
)

select * from final