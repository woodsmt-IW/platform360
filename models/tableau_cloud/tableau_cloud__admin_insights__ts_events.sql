{{ config(
    materialized='incremental',
    unique_key="event_key"
) }}

with source_data as (
    select 
        (raw_json:event_id::varchar || '-' || SPLIT_PART(_file_name,'/',8) || '-' || SPLIT_PART(_file_name,'/',7)) as event_key
        ,SPLIT_PART(_file_name,'/',7) as client
        ,SPLIT_PART(_file_name,'/',8) as site
        ,raw_json as j
        ,max(_load_time) as _load_time
    from {{ source('raw','tableau_cloud__ts_events') }}
  
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
        ,d.event_key as event_key
        ,d.j:actor_license_role::varchar as actor_license_role
        ,d.j:actor_site_role::varchar as actor_site_role
        ,d.j:actor_user_id::int as actor_user_id
        ,d.j:actor_user_name::varchar as actor_user_name
        ,d.j:admin_insights_publish_timestamp::timestamp as admin_insights_publish_timestamp
        ,d.j:event_date::timestamp as event_date
        ,d.j:event_id::int as event_id
        ,d.j:event_is_failure::boolean as event_is_failure
        ,d.j:event_name::varchar as event_name
        ,d.j:event_type::varchar as event_type
        ,d.j:historical_item_name::varchar as historical_item_name
        ,d.j:historical_item_repository_url::varchar as historical_item_repository_url
        ,d.j:item_id::int as item_id
        ,d.j:item_name::varchar as item_name
        ,d.j:item_owner_email::varchar as item_owner_email
        ,d.j:item_owner_friendly_name::varchar as item_owner_friendly_name
        ,d.j:item_owner_id::int as item_owner_id
        ,d.j:item_owner_name::varchar as item_owner_name
        ,d.j:item_project_name::varchar as item_project_name
        ,d.j:item_repository_url::varchar as item_repository_url
        ,d.j:item_type::varchar as item_type
        ,d.j:podname::varchar as podname
        ,d.j:project_name::varchar as project_name
        ,d.j:schedule_name::varchar as schedule_name
        ,d.j:site_luid::varchar as site_luid
        ,d.j:site_name::varchar as site_name
        ,d.j:workbook_name::varchar as workbook_name
    FROM
        source_data as d
)

, final as (
    select * 
    from add_metadata
)

select * from final
