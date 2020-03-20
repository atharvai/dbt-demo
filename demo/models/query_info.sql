{{ config(
        materialized='incremental',
        sort_type='interleaved',
        sort=['starttime', 'query'],
        dist='even',
        post_hook=[
            'grant select on {{ this }} to group end_users',
            'grant select on {{ this }} to group automated_processes'
        ]
    )
}}

SELECT u.usename::varchar as username
     , u.usesuper
     , q.userid
     , q.query
     , q.starttime
     , q.endtime
     , datediff('second', q.starttime, q.endtime) as duration_sec
     , q.querytxt::varchar
     , q.aborted
     , q.concurrency_scaling_status
     , q.label
FROM stl_query q
JOIN pg_user u ON q.userid = u.usesysid

{% if is_incremental() %}
WHERE endtime > (select max(endtime) from {{ this }})
{% endif %}
