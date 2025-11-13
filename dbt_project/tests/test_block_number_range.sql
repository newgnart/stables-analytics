-- Test that block numbers are within expected range

SELECT block_number
FROM {{ ref('fct_transfer') }}
WHERE
    block_number < 0
    OR block_number > 999999999  -- reasonable upper bound
