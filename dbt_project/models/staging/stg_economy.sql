with source as (
    select * from {{ source('gtd_source', 'raw_property_index') }}
)

select
    -- 1. Standardisasi Nama Kolom
    year,
    real_house_price_index as property_index,

    -- 2. Cleaning Nama Negara (Contoh jika ada perbedaan)
    case 
        when country = 'United States of America' then 'United States'
        when country = 'Korea' then 'South Korea'
        else country 
    end as country_name

from source
where year > 1970