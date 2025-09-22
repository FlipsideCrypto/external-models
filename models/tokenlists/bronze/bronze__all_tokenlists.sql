{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'all_tokenlists_id',
    tags = ['tokenlists']
) }}

WITH calls AS ({% for item in range(5) %}
    (

    SELECT
        api_url, 
        {{ target.database }}.live.udf_api('GET', api_url,{},{}) AS request, 
        INDEX AS row_num, 
        SYSDATE() AS _inserted_timestamp
    FROM
        {{ ref('bronze__verified_tokenlist_seed') }}
    WHERE
        is_enabled
        AND row_num BETWEEN {{ item * 10 + 1 }}
        AND {{(item + 1) * 10 }}

) {% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %})
SELECT
    api_url,
    request,
    request :data :keywords :: VARIANT AS keywords,
    request :data :logoURI :: STRING AS logo_uri,
    request :data :name :: STRING AS list_name,
    request :data :tags :: VARIANT AS list_tags,
    row_num,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['api_url','request']
    ) }} AS all_tokenlists_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp
FROM
    calls
