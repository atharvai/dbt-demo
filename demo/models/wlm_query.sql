{{ config(
        materialized='incremental',
        sort_type='compound',
        sort=['query'],
        dist='even',
        post_hook=[
            'grant select on {{ this }} to group end_users',
            'grant select on {{ this }} to group automated_processes'
        ]
    )
}}

SELECT TRIM(DATABASE) AS db,
       w.query,
       SUBSTRING(q.querytxt,1,100) AS querytxt,
       w.queue_start_time,
       w.service_class AS class,
       w.slot_count AS slots,
       w.total_queue_time / 1000000 AS queue_seconds,
       w.total_exec_time / 1000000 exec_seconds,
       (w.total_queue_time + w.total_exec_time) / 1000000 AS total_seconds
FROM stl_wlm_query w
LEFT JOIN stl_query q
         ON q.query = w.query
        AND q.userid = w.userid
WHERE w.total_queue_time > 0
AND   w.userid > 1

{% if is_incremental() %}
AND q.query NOT IN (SELECT query from {{ this }})
{% endif %}
