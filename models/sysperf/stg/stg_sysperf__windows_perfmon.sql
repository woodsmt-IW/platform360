{{ config(
    materialized='incremental'
) }}

with source_data as (
    select 
        *
    from {{ ref('sysperf__windows_perfmon') }}
  
    {% if is_incremental() %}
    
        where record_id > (select coalesce(max(record_id),0) from {{ this }})

    {% endif %}
)

,   sysinfo as (
        SELECT * FROM {{ ref('sysperf__windows_sysinfo')}}
)

,   join_sysinfo as (
    SELECT
        s.*
        ,i.os as os
        ,i.osver as osver
        ,i.proc_cores as proc_cores
        ,i.proc_lp as proc_lp
        ,i.ramgb as ramgb
        ,i.updays as updays

    FROM source_data as s
    LEFT JOIN sysinfo as i ON
        s.record_id = i.record_id
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
        ,val/100 as val
    
    FROM join_sysinfo
        where L1 = 'processor'
        and L2 = '% processor time'
    )

,  cpu_proc as (
    SELECT 
        m.record_id
        ,m.client
        ,m.resource
        ,m.hostname  
        ,m.timestamp
        ,'cpu by process' as L1
        ,m.L3 as L2
        ,NULL as L3
        ,m.val
    
    FROM join_sysinfo as m
    LEFT JOIN cpu_util as c on
        m.record_id = c.record_id
        and m.timestamp = c.timestamp
    where m.L1 = 'process'
        and m.L2 = '% processor time'
        and m.L3 not in ('idle','_total', 'postgres')
        and m.val >= 1  -- exclude idle procs
        and m.val < (proc_lp * c.val * 100) -- exclude over-100%-utilization process reads
    )

,  cpu_proc_postgres as (
    SELECT 
        m.record_id
        ,m.client
        ,m.resource
        ,m.hostname  
        ,m.timestamp
        ,'cpu by process' as L1
        ,m.L3 as L2
        ,NULL as L3
        ,max(m.val)
    
    FROM join_sysinfo as m
    LEFT JOIN cpu_util as c on
        m.record_id = c.record_id
        and m.timestamp = c.timestamp
    where m.L1 = 'process'
        and m.L2 = '% processor time'
        and m.L3 = 'postgres'
        and m.val >= 1  -- exclude idle procs
        and m.val < (proc_lp * c.val * 100) -- exclude over-100%-utilization process reads
    group by 1,2,3,4,5,6,7,8
)

,  disk_perf as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'disk performance' as L1
        ,CASE 
            WHEN L2 = 'avg. disk queue length' THEN 'active time'
            WHEN L2 = 'avg. disk sec/read' THEN 'read latency ms'
            WHEN L2 = 'avg. disk sec/write' THEN 'write latency ms'
            WHEN L2 = 'disk read bytes/sec' THEN 'read throughput mbytes per sec'
            WHEN L2 = 'disk write bytes/sec' THEN 'write throughput mbytes per sec'
            WHEN L2 = 'disk reads/sec' THEN 'read iops'
            WHEN L2 = 'disk writes/sec' THEN 'write iops'
            END as L2
        ,L3
        ,CASE 
            WHEN L2 = 'avg. disk queue length' THEN val
            WHEN L2 = 'avg. disk sec/read' THEN val * 1000
            WHEN L2 = 'avg. disk sec/write' THEN val * 1000
            WHEN L2 = 'disk read bytes/sec' THEN  val / 1024 / 1024
            WHEN L2 = 'disk write bytes/sec' THEN val / 1024 / 1024
            WHEN L2 = 'disk reads/sec' THEN val
            WHEN L2 = 'disk writes/sec' THEN val
            END as val
    FROM join_sysinfo
    where L1 = 'logicaldisk'
        and L2 in ('avg. disk queue length','avg. disk sec/read','avg. disk sec/write','disk read bytes/sec','disk write bytes/sec', 'disk reads/sec','disk writes/sec')
        and L3 not like 'harddiskvolume%'
        and L3 not in ('_total')
    )

,  disk_space_stg as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,L3 
        ,max(CASE WHEN L2 = '% free space' THEN val END) as free_space
        ,max(CASE WHEN L2 = 'free megabytes' THEN val END) as free_mb
    
    FROM join_sysinfo
    where L1 = 'logicaldisk'
        and L2 in ('% free space', 'free megabytes' )
        and L3 not like 'harddiskvolume%'
        and L3 not in ('_total')
    GROUP BY 1,2,3,4,5,6
    )

,  disk_space_util as (
    SELECT  
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'disk space' as L1
        ,'pct utilization' as L2
        ,L3
        ,(100-free_space)/100 as val    
    FROM disk_space_stg
    )

,  disk_space_used_gb as (
    SELECT  
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'disk space' as L1
        ,'used gbytes' as L2
        ,L3
        ,( div0(free_mb , (free_space/100) ) - free_mb )/ 1024 as val
    
    FROM disk_space_stg
    )

,  disk_space_size_gb as (
    SELECT  
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'disk space' as L1
        ,'size gbytes' as L2
        ,L3
        , div0(free_mb , (free_space/100) ) / 1024 as val
    FROM disk_space_stg
    )

,  mem_util as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'memory' as L1
        ,'pct utilization' as L2
        ,NULL as L3
        ,val/100 as val
    FROM join_sysinfo
        where L1 = 'memory'
        and L2 = '% committed bytes in use'
    )

,  mem_detail_stg as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,max(case when L2 = 'commit limit' then val end) as commit_limit
        ,max(case when L2 = 'committed bytes' then val end) as committed_bytes
        ,max(case when L2 = 'free & zero page list bytes' then val end) as free_bytes
        ,max(ramgb) as ramgb
    FROM join_sysinfo
    where L1 = 'memory'
        and L2 in ('commit limit','committed bytes','free & zero page list bytes') 
    group by 1,2,3,4,5
    )

, win_ram_total as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'memory' as L1
        ,'Total GB' as L2
        ,NULL as L3
        ,ramgb as val
    FROM mem_detail_stg
)

, win_ram_commit as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'memory' as L1
        ,'Committed GB' as L2
        ,NULL as L3
        ,committed_bytes / 1024 / 1024 / 1024 as val
    FROM mem_detail_stg
)

, win_ram_free as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'memory' as L1
        ,'Free GB' as L2
        ,NULL as L3
        ,free_bytes / 1024 / 1024 / 1024 as val
    FROM mem_detail_stg
)

, win_ram_cached as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'memory' as L1
        ,'Cached GB' as L2
        ,NULL as L3
        ,(commit_limit - committed_bytes - free_bytes) / 1024 / 1024 / 1024 as val
    FROM mem_detail_stg
)

,  mem_proc as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'memory by process' as L1
        ,L3 as L2
        ,NULL as L3
        ,sum(div0(val , ramgb)) /1024/1024/1024 as val
    FROM join_sysinfo
    where L1 = 'process'
        and L2 = 'private bytes'
        and L3 not in ('_total')
        and val > 10000000
    group by 1,2,3,4,5,6,7,8
    )

,  net as (
    SELECT 
        record_id
        ,client
        ,resource
        ,hostname  
        ,timestamp
        ,'network' as L1
        ,CASE 
            WHEN L2 = 'bytes received/sec' THEN 'inbound mbps' 
            WHEN L2 = 'bytes sent/sec' THEN 'outbound mbps'
            END as L2
        ,L3
        ,CASE 
            WHEN L2 = 'bytes received/sec' THEN val / 1024 / 1024
            WHEN L2 = 'bytes sent/sec' THEN val / 1024 / 1024
            END as val
    FROM join_sysinfo
    where L1 = 'network interface'
        and L2 in ('bytes received/sec','bytes sent/sec')
    )

,   os as (
        SELECT
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'os' as L1
            ,os as L2
            ,osver as L3
            ,1 as val
        FROM join_sysinfo
        where L1 = 'processor'
        and L2 = '% processor time'
)

,   num_lps as (
    SELECT
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'cpu' as L1
            ,'logical processors' as L2
            ,NULL as L3
            ,proc_lp as val
        FROM join_sysinfo
        where L1 = 'processor'
        and L2 = '% processor time'
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
            ,proc_cores as val
        FROM join_sysinfo
        where L1 = 'processor'
        and L2 = '% processor time'
)

,   updays as (
    SELECT
            record_id
            ,client
            ,resource
            ,hostname  
            ,timestamp
            ,'system' as L1
            ,'uptime days' as L2
            ,NULL as L3
            ,updays as val
        FROM join_sysinfo
        where L1 = 'processor'
        and L2 = '% processor time'
)

,   combined as (
    select * from cpu_util
    union all
    select * from cpu_proc
    union all
    select * from cpu_proc_postgres
    union all
    select * from disk_perf
    union all
    select * from disk_space_util
    union all
    select * from disk_space_used_gb
    union all
    select * from disk_space_size_gb
    union all
    select * from mem_util
    union all
    select * from win_ram_total
    union all
    select * from win_ram_commit
    union all
    select * from win_ram_free
    union all
    select * from win_ram_cached
    union all
    select * from mem_proc
    union all
    select * from net
    union all
    select * from os
    union all
    select * from num_lps
    union all
    select * from num_cores
    union all
    select * from updays

)

, final as (
    select * FROM combined
)

select * from final