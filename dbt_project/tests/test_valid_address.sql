-- Test that all addresses are valid Ethereum addresses (42 chars with 0x prefix)

SELECT
    contract_address,
    count(*) AS invalid_count
FROM {{ ref('fct_transfer') }}
WHERE
    contract_address IS null
    OR length(contract_address) != 42
    OR substring(contract_address, 1, 2) != '0x'
GROUP BY contract_address
HAVING count(*) > 0
