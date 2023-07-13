
{{ config(
    materialized='view'
) }}

with source_data as (
    select 
        record_id
        ,_file_name
        ,raw_json as j
    from {{ source('raw','tableau_cloud__groups') }}
  
    {% if is_incremental() %}
    
        where record_id > (select max(record_id) from {{ this }})

    {% endif %}
)

, add_metadata as (
    SELECT
        REPLACE(SPLIT_PART(d._file_name,'/',7),'x')::int as client
        ,SPLIT_PART(d._file_name,'/',8)::varchar as site
        ,d.j:admin_insights_publish_timestamp::timestamp as timestamp
        ,d.j:group_name::varchar as group_name
        ,d.j:group_minimum_site_role::varchar as group_min_site_role
        ,d.j:group_is_license_on_sign_in::boolean as group_assign_license_on_signin
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
