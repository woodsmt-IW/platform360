{{ config(
    materialized='incremental'
) }}

with source_data as (
    select 
        *
    from {{ ref('sysperf__linux_stats') }}
  
    {% if is_incremental() %}
    
        where record_id > (select coalesce(max(record_id),0) from {{ this }})

    {% endif %}

)

, map_ids as (
    SELECT
        record_id
        ,CASE len(client) 
            WHEN 31 THEN concat( left(client,13), 'x' , right(client,18) )
            ELSE client 
            END as client
        ,CASE len(resource)
            WHEN 31 THEN concat( left(resource,13), 'x' , right(resource,18) )
            ELSE resource 
            END as resource
        ,hostname
        ,timestamp
        ,L1
        ,L2
        ,L3
        ,L4
        ,val
    FROM source_data
)

,   cpu_util as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'cpu' as L1
            ,'pct utilization' as L2
            ,NULL as L3
            ,(100-val)/100 as val
         FROM map_ids
        where L1 = 'cpu'
        and L2 = 'cpu_idl'
)

,   cpu_proc as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'cpu by process' as L1
            ,L3 as L2 -- process name
            ,NULL as L3
            ,val
         FROM map_ids
        where L1 = 'process'
        and L2 = '% processor time'
)

,   disk_perf as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'disk performance' as L1
            ,CASE 
                WHEN L2 = 'active time pct' THEN 'active time' 
                ELSE L2
                END as L2
            ,L3
            ,val
         FROM map_ids
        where L1 = 'disk'
        and L2 in ('active time pct','read iops', 'read latency ms', 'read throughput mbytes per sec', 'write iops', 'write latency ms', 'write throughput mbytes per sec')
        and L3 not like '/snap/%'
        and L3 not like '/boot%'
    )

,   disk_space as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'disk space' as L1
            ,CASE 
                WHEN L2 = 'used kbytes' THEN 'used gbytes' 
                WHEN L2 = 'size kbytes' THEN 'size gbytes' 
                WHEN L2 = '% utilization' THEN 'pct utilization'
                ELSE L2
                END as L2
            ,L3
            ,CASE 
                WHEN L2 = 'used kbytes' THEN val /1024/1024 
                WHEN L2 = 'size kbytes' THEN val /1024/1024 
                ELSE val
                END as val
         FROM map_ids
        where L1 = 'disk mounts'
        and L2 in ('% utilization','used kbytes', 'size kbytes')
        and L3 not like '/snap/%'
        and L3 not like '/boot%'
    )

,   mem_util as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'memory' as L1
            ,'pct utilization' as L2
            ,NULL as L3
            ,(max(case when L2 = 'mem_used' then val end) / max(case when L2 = 'mem_tot' then val end)) as val
         FROM map_ids
        where L1 = 'memory'
        and L2 in ('mem_tot','mem_used')
        group by 1,2,3,4,5,6,7,8
    )

,   mem_detail as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'memory' as L1
            ,CASE 
                WHEN L2 = 'mem_tot' then 'Total GB'
                WHEN L2 = 'mem_used' then 'Committed GB'
                WHEN L2 = 'mem_free' then 'Free GB'
                WHEN L2 = 'mem_buff' then 'Cached GB'
                END as L2
            ,NULL as L3
            ,val / 1024 / 1024 as val
         FROM map_ids
        where L1 = 'memory'
        and L2 in ('mem_tot','mem_used','mem_free','mem_buff')
    )

,   mem_proc as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'memory by process' as L1
            ,L3 as L2
            ,NULL as L3
            ,sum(val) / 100 as val
         FROM map_ids
        where L1 = 'process'
        and L2 = '% memory utilization'
        group by 1,2,3,4,5,6,7,8
    )

,   net as (
        SELECT 
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'network' as L1
            ,CASE 
                WHEN L2 = 'inbound kbps' THEN 'inbound mbps' 
                WHEN L2 = 'outbound kbps' THEN 'outbound mbps'
                END as L2
            ,L3
            ,CASE 
                WHEN L2 = 'inbound kbps' THEN val / 1024 
                WHEN L2 = 'outbound kbps' THEN val / 1024 
                END as val
         FROM map_ids
        where L1 = 'network'
            and L3 not in ('lo')
    )

,   os as (
        SELECT
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'os' as L1
            ,L2
            ,L3
            ,1 as val
        FROM map_ids
        WHERE L1 = 'OS'
)

,   num_cpu as (
    SELECT
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'cpu' as L1
            ,'logical processors' as L2
            ,NULL as L3
            ,val
        FROM map_ids
        WHERE L1 = 'cpu'
            and L2 = 'num_procs'
)
,   num_cores as (
    SELECT
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'cpu' as L1
            ,'cpu cores' as L2
            ,NULL as L3
            ,val
        FROM map_ids
        WHERE L1 = 'cpu'
            and L2 = 'num_cores'
)

, combined as (
    select * from cpu_util
    UNION ALL
    select * from cpu_proc
    UNION ALL
    select * from disk_perf
    UNION ALL
    select * from disk_space
    UNION ALL
    select * from mem_util
    UNION ALL
    select * from mem_detail
    UNION ALL
    select * from mem_proc
    UNION ALL
    select * from net
    UNION ALL
    select * from os
    UNION ALL
    select * from num_cpu
    UNION ALL
    select * from num_cores
)

, final as (
    select * FROM combined
)

select * from final




