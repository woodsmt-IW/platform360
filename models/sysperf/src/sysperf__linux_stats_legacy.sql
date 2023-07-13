
{{ config(
    materialized='incremental'
) }}

with source_data as (
    select 
        record_id 
        ,raw_json as c
    from {{ source('raw','sysperf__linux_stats_legacy') }}
  
    {% if is_incremental() %}
    
        where record_id > (select coalesce(max(record_id),0) from {{ this }})

    {% endif %}
)

, flatten as (
    SELECT
        record_id
        ,f.value:cc::varchar as cc
        ,f.value:hostname::varchar as hostname
        ,f.value:timestamp::timestamp as timestamp
        ,f.value:L1::varchar as L1
        ,f.value:L2::varchar as L2
        ,f.value:L3::varchar as L3
        ,f.value:L4::varchar as L4
        ,f.value:val::double as val
    FROM source_data as d,
    lateral flatten(input => d.c, path => '') f
)

, filter as (
    SELECT * FROM flatten
    WHERE
        (L1 <> 'disk' OR ( L3 not like '/snap/%' and L3 not like '/boot%' ))
)

select * 
from filter
