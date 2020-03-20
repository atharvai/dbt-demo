{{ config(
        materialized='incremental',
        dist='even',
        post_hook=[
            'grant select on {{ this }} to group end_users',
            'grant select on {{ this }} to group automated_processes'
        ]
    )
}}

SELECT q.*,w.class
FROM {{ ref('query_info') }} q
JOIN {{ref('wlm_query')}} w on q.query=w.query
