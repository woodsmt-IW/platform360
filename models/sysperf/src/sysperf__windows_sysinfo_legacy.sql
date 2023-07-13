
{{ config(
    materialized='incremental'
) }}

with source_data as (

    select 
        record_id 
        ,raw_json as c
    from {{ source('raw','sysperf__windows_perfmon_legacy') }}
    {% if is_incremental() %}
    
        where record_id > (select coalesce(max(record_id),0) from {{ this }})

    {% endif %}
)

, parse as (
    SELECT
        d.c:cc::varchar as cc
        ,SPLIT_PART(d.c:counters[0].Path,'\\',3)::varchar as hostname
        ,TO_TIMESTAMP_NTZ(SUBSTRING(d.c:counters[0].Timestamp,7,13))::timestamp as TIMESTAMP 
        ,d.c:os::text as os
        ,d.c:osver::text as osver
        ,'tableauserver/windowsperfmon' as platform_type
        ,d.c:proc_cores::int as proc_cores
        ,d.c:proc_lp::int as proc_lp
        ,d.c:ramgb::int as ramgb
        ,d.c:updays::int as updays
        ,d.record_id::int as record_id
    FROM source_data as d
)

select *
from parse
