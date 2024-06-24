WITH latest_tvl AS (
  SELECT
    contract_address,
    symbol,
    MAX_BY(balance, day) AS tvl_raw,
    MAX_BY(balance_usd, day) AS tvl_usd
  FROM query_3793148
  GROUP BY
    contract_address,
    symbol
)

    SELECT
      SUM(tvl_usd / 1e6) AS total_tvl,
      SUM(CASE WHEN contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 THEN tvl_usd / 1e6 ELSE 0 END) AS bridged_eth
    FROM latest_tvl
