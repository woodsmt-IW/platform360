
{{ config(
    materialized='incremental'
) }}

with source_data as (

    select 
        record_id 
        ,raw_json as c
    from {{ source('raw','sysperf__windows_perfmon') }}
    {% if is_incremental() %}
    
        where record_id > (select coalesce(max(record_id),0) from {{ this }})

    {% endif %}
)

, parse as (
    SELECT
        d.c:client::varchar as client
        ,d.c:resource::varchar as resource
        ,d.c:hostname::text as hostname
        ,TO_TIMESTAMP_NTZ(d.c:unixtime::int)::timestamp as timestamp
        ,d.c:os::text as os
        ,d.c:osver::text as osver
        ,d.c:platform_type::text as platform_type
        ,d.c:proc_cores::int as proc_cores
        ,d.c:proc_lp::int as proc_lp
        ,d.c:ramgb::int as ramgb
        ,d.c:updays::int as updays
        ,d.record_id::int as record_id
    FROM source_data as d
)

, map_ids as (
    SELECT
        CASE len(client) 
            WHEN 31 THEN concat( left(client,13), 'x' , right(client,18) )
            ELSE client 
            END as client
        ,CASE len(resource)
            WHEN 31 THEN concat( left(resource,13), 'x' , right(resource,18) )
            ELSE resource 
            END as resource
        ,hostname
        ,timestamp
        ,os
        ,osver
        ,platform_type
        ,proc_cores
        ,proc_lp
        ,ramgb
        ,updays
        ,record_id
    FROM parse
)

select *
from map_ids
