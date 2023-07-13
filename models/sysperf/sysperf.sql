{{ config(materialized="view") }}

with
    src_lx as (select * from {{ ref("stg_sysperf__linux_stats") }}),
    src_lx_leg as (select * from {{ ref("stg_sysperf__linux_stats_legacy") }}),
    src_win as (select * from {{ ref("stg_sysperf__windows_perfmon") }}),
    src_win_leg as (select * from {{ ref("stg_sysperf__windows_perfmon_legacy") }}),
    combined as (
        select *, 'Linux' as os, false as legacy
        from src_lx
        union all
        select *, 'Linux' as os, true as legacy
        from src_lx_leg
        union all
        select *, 'Windows' as os, false as legacy
        from src_win
        union all
        select *, 'Windows' as os, true as legacy
        from src_win_leg
    )

select *
from combined
