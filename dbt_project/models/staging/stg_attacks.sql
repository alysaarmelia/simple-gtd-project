with raw_data as (
    select * from {{ source('gtd_source', 'raw_gtd') }}
)

select
    eventid as event_id,
    iyear as year,
    imonth as month,
    iday as day,
    country_txt as country_name,
    region_txt as region_name,
    city as city_name,
    latitude,
    longitude,
    coalesce(nkill, 0) as killed,
    coalesce(nwound, 0) as wounded,
    attacktype1_txt as attack_type,
    targtype1_txt as target_type,
    gname as group_name,
    weaptype1_txt as weapon_type,
    summary

from raw_data
where iyear > 1900