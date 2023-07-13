
{{ config(
    materialized='view'
) }}

with source_data as (
    select 
        record_id
        ,_file_name
        ,_load_time
        ,raw_json as j
    from {{ source('raw','tableau_cloud__site_info') }}
  
    {% if is_incremental() %}
    
        where record_id > (select max(record_id) from {{ this }})

    {% endif %}
)

, add_metadata as (
    SELECT
        REPLACE(SPLIT_PART(d._file_name,'/',7),'x')::int as client
        ,SPLIT_PART(d._file_name,'/',8)::varchar as site
        ,d._load_time::timestamp as timestamp
        ,d.j:site::variant as full_info
        ,d.j:site.storageQuota::double as storage_quota_mb
        ,d.j:site.usage.storage::double as storage_usage_mb
        ,d.j:site.userQuota::int as total_user_capacity
        ,d.j:site.usage.numUsers::int as total_users
        ,d.j:site.tierCreatorCapacity::int as license_capacity_creator
        ,d.j:site.usage.numCreators::int as license_usage_creator
        ,d.j:site.tierExplorerCapacity::int as license_capacity_explorer
        ,d.j:site.usage.numExplorers::int as license_usage_explorer
        ,d.j:site.tierViewerCapacity::int as license_capacity_viewer
        ,d.j:site.usage.numViewers::int as license_usage_viewer
        ,d.j:site.adminMode::varchar as admin_mode
        ,d.j:site.allowSubscriptionAttachments::boolean as allow_Subscription_attachments
        ,d.j:site.askDataMode::varchar as ask_data_mode
        ,d.j:site.authoringEnabled::boolean as authoring_enabled
        ,d.j:site.autoSuspendRefreshEnabled::boolean as auto_suspend_refresh_enabled
        ,d.j:site.autoSuspendRefreshInactivityWindow::int as auto_suspend_refresh_inactivity_window
        ,d.j:site.cacheWarmupEnabled::boolean as cache_warmup_enabled
        ,d.j:site.catalogingEnabled::boolean as cataloging_enabled
        ,d.j:site.commentingEnabled::boolean as commenting_enabled
        ,d.j:site.commentingMentionsEnabled::boolean as commenting_mentions_enabled
        ,d.j:site.contentUrl::varchar as content_url
        ,d.j:site.customSubscriptionEmailEnabled::boolean as custom_subscription_email_enabled
        ,d.j:site.customSubscriptionFooterEnabled::boolean as custom_subscription_footer_enabled
        ,d.j:site.dataAccelerationMode::varchar as data_acceleration_mode
        ,d.j:site.dataAlertsEnabled::boolean as data_alerts_enabled
        ,d.j:site.derivedPermissionsEnabled::boolean as derived_permissions_enabled
        ,d.j:site.disableSubscriptions::boolean as disable_subscriptions
        ,d.j:site.dqwSubscriptionsEnabled::boolean as dqw_subscriptions_enabled
        ,d.j:site.editingFlowsEnabled::boolean as editing_flows_enabled
        ,d.j:site.explainDataEnabled::boolean as explain_data_enabled
        ,d.j:site.extractEncryptionMode::varchar as extract_encryption_mode
        ,d.j:site.flowAutoSaveEnabled::boolean as flow_auto_save_enabled
        ,d.j:site.flowsEnabled::boolean as flows_enabled
        ,d.j:site.guestAccessEnabled::boolean as guest_access_enabled
        ,d.j:site.id::varchar as uuid
        ,d.j:site.loginBasedLicenseManagementEnabled::boolean as login_based_license_management_enabled
        ,d.j:site.name::varchar as name
        ,d.j:site.namedSharingEnabled::boolean as named_sharing_enabled
        ,d.j:site.personalSpaceEnabled::boolean as personal_space_enabled
        ,d.j:site.requestAccessEnabled::boolean as request_access_enabled
        ,d.j:site.revisionHistoryEnabled::boolean as revision_history_enabled
        ,d.j:site.revisionLimit::int as revision_limit
        ,d.j:site.runNowEnabled::boolean as run_now_enabled
        ,d.j:site.schedulingFlowsEnabled::boolean as scheduling_flows_enabled
        ,d.j:site.selfServiceScheduleForRefreshEnabled::boolean as self_service_schedule_for_refresh_enabled
        ,d.j:site.state::varchar as state
        ,d.j:site.subscribeOthersEnabled::boolean as subscribe_others_enabled
        ,d.j:site.useDefaultTimeZone::boolean as use_default_time_zone
        ,d.j:site.userVisibilityMode::varchar as user_visibility_mode
        ,d.j:site.webExtractionEnabled::boolean as web_extraction_enabled

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

select * 
from flag_current
