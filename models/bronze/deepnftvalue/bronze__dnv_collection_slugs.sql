{{ config(
    materialized = 'table'
) }}

SELECT
    'boredapeyachtclub' AS collection_slug,
    '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d' AS contract_address,
    '2022-01-01' AS created_at
UNION
SELECT
    'cryptopunks',
    '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb',
    '2022-01-01'
UNION
SELECT
    'azuki',
    '0xed5af388653567af2f388e6224dc7c4b3241c544',
    '2022-01-01'
