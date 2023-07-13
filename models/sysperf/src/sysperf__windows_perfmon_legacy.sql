
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

-- Flatten the JSON from one row per reading to one row per counter
, flatten as (
    SELECT
        record_id
        ,d.c:cc::varchar as cc
        ,f.value as counter
    FROM source_data as d,
    lateral flatten(input => d.c, path => 'counters') f
)

-- Split out the Path value into the different dimensions
, parse as (
    SELECT
        record_id
        ,cc
        ,SPLIT_PART(counter:Path,'\\',3)::varchar as hostname
        ,TO_TIMESTAMP_NTZ(SUBSTRING(counter:Timestamp,7,13))::timestamp as TIMESTAMP 
        ,SPLIT_PART(SPLIT_PART(counter:Path,'\\',4),'(',1)::varchar as L1
        ,SPLIT_PART(counter:Path,'\\',5)::varchar as L2
        ,counter:InstanceName::varchar as L3
        ,SPLIT_PART(SPLIT_PART(SPLIT_PART(counter:Path,'\\',4),'#',2),')',1)::varchar as L4
        ,counter:CookedValue::double as VAL
    FROM flatten as f
)

-- Filter out the tiny values
, filter as (
    select * from parse
    where 
        (L1 <> 'process' or VAL > 1)  -- only active processes
        and (L2 <> 'private bytes' or VAL > 10000000)  -- only processes with >50MB of memory usage
        and (L1 not in ('logicaldisk','process') or L3 not in ('idle','_total')) -- remove unhelpful rows
        and (L1 <> 'logicaldisk' or L3 not like 'harddiskvolume%')
        and (L1 <> 'network' or L3 <> 'lo')      
) 

select *
from filter
