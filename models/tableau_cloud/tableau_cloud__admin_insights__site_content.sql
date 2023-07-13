
{{ config(
    materialized='view'
) }}

with source_data as (
    select 
        record_id
        ,_file_name
        ,raw_json as j
    from {{ source('raw','tableau_cloud__site_content') }}
  
    {% if is_incremental() %}
    
        where record_id > (select max(record_id) from {{ this }})

    {% endif %}
)

, add_metadata as (
    SELECT
        REPLACE(SPLIT_PART(d._file_name,'/',7),'x')::int as client
        ,SPLIT_PART(d._file_name,'/',8)::varchar as site
        ,d.j:admin_insights_publish_timestamp::timestamp as timestamp
        ,d.j:created_at::timestamp as created_at
        ,d.j:last_access_date::timestamp as last_access_date
        ,d.j:last_published_at::timestamp as last_published_at
        ,d.j:updated_at::timestamp as updated_at
        ,d.j:item_hyperlink::varchar as item_hyperlink
        ,d.j:item_luid::varchar as item_luid
        ,d.j:item_name::varchar as item_name
        ,d.j:item_type::varchar as item_type
        ,d.j:owner_email::varchar as owner_email
        ,d.j:site_hyperlink::varchar as site_hyperlink
        ,d.j:site_luid::varchar as site_luid
        ,d.j:site_name::varchar as site_name
        ,d.j:item_id::int as item_id
        ,d.j:project_level::int as project_level
        ,d.j:storage_quota_bytes::int as storage_quota_bytes
        ,d.j:data_source_content_type::varchar as data_source_content_type
        ,d.j:data_source_database_type::varchar as data_source_database_type
        ,d.j:data_source_is_certified::boolean as data_source_is_certified
        ,d.j:description::varchar as description
        ,d.j:first_published_at::timestamp as first_published_at
        ,d.j:has_incrementable_extract::boolean as has_incrementable_extract
        ,d.j:has_refresh_scheduled::boolean as has_refresh_scheduled
        ,d.j:has_refreshable_extract::boolean as has_refreshable_extract
        ,d.j:is_data_extract::boolean as is_data_extract
        ,d.j:extracts_incremented_at::timestamp as extracts_incremented_at
        ,d.j:extracts_refreshed_at::timestamp as extracts_refreshed_at
        ,d.j:item_parent_project_id::int as item_parent_project_id
        ,d.j:item_parent_project_level::int as item_parent_project_level
        ,d.j:item_parent_project_name::varchar as item_parent_project_name
        ,d.j:item_parent_project_owner_email::varchar as item_parent_project_owner_email
        ,d.j:item_revision::int as item_revision
        ,d.j:size_bytes::int as size_bytes
        ,d.j:top_parent_project_name::varchar as top_parent_project_name
        ,d.j:view_title::varchar as view_title
        ,d.j:view_type::varchar as view_type
        ,d.j:view_workbook_id::int as view_workbook_id
        ,d.j:view_workbook_name::varchar as view_workbook_name
        ,d.j:workbook_shows_sheets_as_tabs::boolean as workbook_shows_sheets_as_tabs

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
