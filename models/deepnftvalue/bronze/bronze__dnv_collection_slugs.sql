{{ config(
    materialized = 'table',
    tags = ['stale']
) }}

SELECT
    'boredapeyachtclub' AS collection_slug,
    '0xbc4ca0eda7647a8ab7c2061c2e118a18a936f13d' AS contract_address,
    '2022-06-01' AS created_at,
    10000 AS total_supply
UNION
SELECT
    'cryptopunks',
    '0xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb',
    '2021-01-01',
    10000
UNION
SELECT
    'azuki',
    '0xed5af388653567af2f388e6224dc7c4b3241c544',
    '2022-06-01',
    10000
UNION
SELECT
    'mutant-ape-yacht-club',
    '0x60E4d786628Fea6478F785A6d7e704777c86a7c6',
    '2022-06-01',
    20000
UNION
SELECT
    'nakamigos',
    '0xd774557b647330c91bf44cfeab205095f7e6c367',
    '2023-03-22',
    20000
UNION
SELECT
    'pudgypenguins',
    '0xbd3531da5cf5857e7cfaa92426877b022e612cf8',
    '2022-04-21',
    8888
UNION
SELECT
    'wrapped-cryptopunks',
    '0xb7f7f6c52f2e2fdb1963eab30438024864c313f6',
    '2021-01-01',
    1000
