
{{ config(
    materialized='view'
) }}

with source_data as (
    select 
        record_id
        ,_file_name
        ,raw_json as j
    from {{ source('raw','tableau_cloud__ts_users') }}
  
    {% if is_incremental() %}
    
        where record_id > (select max(record_id) from {{ this }})

    {% endif %}
)

, add_metadata as (
    SELECT
        REPLACE(SPLIT_PART(d._file_name,'/',7),'x')::int as client
        ,SPLIT_PART(d._file_name,'/',8)::varchar as site
        ,d.j:admin_insights_publish_timestamp::timestamp as timestamp
        ,d.j:access_data_source_events_count::int as access_data_source_events_count
        ,d.j:access_view_events::int as access_view_events
        ,d.j:created_at::timestamp as created_at
        ,d.j:data_source_access_count_total::int as data_source_access_count_total
        ,d.j:data_source_access_count_unique_users::int as data_source_access_count_unique_users
        ,d.j:days_since_last_login::int as days_since_last_login
        ,d.j:last_datasource_access_date::timestamp as last_datasource_access_date
        ,d.j:last_datasource_publish_date::timestamp as last_datasource_publish_date
        ,d.j:last_login_date::timestamp as last_login_date
        ,d.j:last_view_access_date::timestamp as last_view_access_date
        ,d.j:last_workbook_publish_date::timestamp as last_workbook_publish_date
        ,d.j:owned_certified_data_sources::int as owned_certified_data_sources
        ,d.j:owned_data_sources::int as owned_data_sources
        ,d.j:owned_data_sources_size::int as owned_data_sources_size
        ,d.j:owned_projects::int as owned_projects
        ,d.j:owned_views::int as owned_views
        ,d.j:owned_workbooks::int as owned_workbooks
        ,d.j:owned_workbooks_size::double as owned_workbooks_size
        ,d.j:podname::varchar as podname
        ,d.j:publish_data_source_events::int as publish_data_source_events
        ,d.j:publish_workbook_events::int as publish_workbook_events
        ,d.j:site_create_date::timestamp as site_create_date
        ,d.j:site_creator_capacity::int as site_creator_capacity
        ,d.j:site_explorer_capacity::int as site_explorer_capacity
        ,d.j:site_luid::varchar as site_luid
        ,d.j:site_name::varchar as site_name
        ,d.j:site_role::varchar as site_role
        ,d.j:site_user_quota::int as site_user_quota
        ,d.j:site_viewer_capacity::int as site_viewer_capacity
        ,d.j:user_account_age::int as user_account_age
        ,d.j:user_email::varchar as user_email
        ,d.j:user_id::int as user_id
        ,d.j:user_luid::varchar as user_luid
        ,d.j:view_access_count_total::int as view_access_count_total
        ,d.j:view_access_count_unique_users::int as view_access_count_unique_users
    FROM
        source_data as d
)

, max_date as (
    SELECT 
        client
        ,site
        ,max(timestamp) as timestamp
    FROM
        add_metadata
    GROUP BY 1,2
)

, flag_current as (
    SELECT
        d.*
        ,case when d.timestamp = m.timestamp then true else false end as is_current
    FROM add_metadata as d
    LEFT JOIN max_date as m 
        on d.client = m.client
        and d.site = m.site
)

select distinct * 
from flag_current
