{{ config(
    materialized = "table",
    tags = ['core']
) }}
{{ dbt_date.get_date_dimension(
    '2017-01-01',
    '2027-12-31'
) }}
