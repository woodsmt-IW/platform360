
{{ config(
    materialized='incremental'
) }}

with source_data as (
    select 
        record_id 
        ,raw_json as c
    from {{ source('raw','sysperf__linux_stats') }}
  
    {% if is_incremental() %}
    
        where record_id > (select coalesce(max(record_id),0) from {{ this }})

    {% endif %}
)

, flatten as (
    SELECT
        record_id
        ,f.value:client::varchar as client
        ,f.value:resource::varchar as resource
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

, map_ids as (
    SELECT
        record_id
        ,CASE len(client) 
            WHEN 31 THEN concat( left(client,13), 'x' , right(client,18) )
            ELSE client 
            END as client
        ,CASE len(resource)
            WHEN 31 THEN concat( left(resource,13), 'x' , right(resource,18) )
            ELSE resource 
            END as resource
        ,hostname
        ,timestamp
        ,L1
        ,L2
        ,L3
        ,L4
        ,val
    FROM flatten
    WHERE
        (L1 <> 'disk' OR ( L3 not like '/snap/%' and L3 not like '/boot%' ))
)



select * 
from map_ids
