{{ config(
    materialized='incremental',
    unique_key="job_key"
) }}

with source_data as (
    select 
        (raw_json:job_id::varchar || '-' || SPLIT_PART(_file_name,'/',8) || '-' || SPLIT_PART(_file_name,'/',7)) as job_key
        ,SPLIT_PART(_file_name,'/',7) as client
        ,SPLIT_PART(_file_name,'/',8) as site
        ,raw_json as j
        ,max(_load_time) as _load_time
    from {{ source('raw','tableau_cloud__job_performance') }}
  
    {% if is_incremental() %}
    
        where _load_time > (select max(_load_time) from {{ this }})

    {% endif %}

    group by 1,2,3,4
)

, add_metadata as (
    SELECT
        REPLACE(d.client,'x')::int as client
        ,d.site::varchar as site
        ,d._load_time::timestamp as _load_time
        ,d.job_key as job_key
        ,d.j:completed_at::timestamp as completed_at
        ,d.j:created_at::timestamp as created_at
        ,d.j:item_hyperlink::varchar as item_hyperlink
        ,d.j:item_id::int as item_id
        ,d.j:item_name::varchar as item_name
        ,d.j:item_type::varchar as item_type
        ,d.j:job_duration::double as job_duration
        ,d.j:job_execution_duration::double as job_execution_duration
        ,d.j:job_id::int as job_id
        ,d.j:job_overflow_queued_duration::double as job_overflow_queued_duration
        ,d.j:job_queued_duration::double as job_queued_duration
        ,d.j:job_result::varchar as job_result
        ,d.j:job_type::varchar as job_type
        ,d.j:manual_run::boolean as manual_run
        ,d.j:overflow_queued_at::timestamp as overflow_queued_at
        ,d.j:owner_email::varchar as owner_email
        ,d.j:queued_at::timestamp as queued_at
        ,d.j:schedule_luid::varchar as schedule_luid
        ,d.j:schedule_name::varchar as schedule_name
        ,d.j:site_luid::varchar as site_luid
        ,d.j:site_name::varchar as site_name
        ,d.j:started_at::timestamp as started_at
        ,d.j:was_overflow_queued::boolean as was_overflow_queued
    FROM
        source_data as d
)

, final as (
    select * 
    from add_metadata
)

select * from final
