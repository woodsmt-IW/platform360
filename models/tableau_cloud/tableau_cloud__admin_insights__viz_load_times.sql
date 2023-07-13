
{{ config(
    materialized='incremental',
    unique_key="request_key"
) }}

with source_data as (
    select 
        (raw_json:request_id::varchar || '-' || SPLIT_PART(_file_name,'/',8) || '-' || SPLIT_PART(_file_name,'/',7)) as request_key
        ,SPLIT_PART(_file_name,'/',7) as client
        ,SPLIT_PART(_file_name,'/',8) as site
        ,raw_json as j
        ,max(_load_time) as _load_time
    from {{ source('raw','tableau_cloud__viz_load_times') }}
  
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
        ,d.request_key as request_key        
        ,d.j:duration::double as duration
        ,d.j:http_request_uri::varchar as http_request_uri
        ,d.j:http_user_agent::varchar as http_user_agent
        ,d.j:item_hyperlink::varchar as item_hyperlink
        ,d.j:item_luid::varchar as item_luid
        ,d.j:item_name::varchar as item_name
        ,d.j:item_owner_email::varchar as item_owner_email
        ,d.j:item_repository_url::varchar as item_repository_url
        ,d.j:item_type::varchar as item_type
        ,d.j:project_name::varchar as project_name
        ,d.j:project_owner_user_name::varchar as project_owner_user_name
        ,d.j:request_id::varchar as request_id
        ,d.j:request_time::timestamp as request_time
        ,d.j:site_luid::varchar as site_luid
        ,d.j:status_code::int as status_code
        ,d.j:workbook_name::varchar as workbook_name
        ,d.j:workbook_owner_user_name::varchar as workbook_owner_user_name
    FROM
        source_data as d
)

, final as (
    select * 
    from add_metadata
)

select * from final
